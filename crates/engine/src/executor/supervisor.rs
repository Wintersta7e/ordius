//! Cross-platform supervised subprocess: spawn, capture, terminate.
//!
//! Extracted from `subprocess.rs` so the environment probe path can
//! reuse the same Job Object (Windows) / process-group (Unix) tear-
//! down semantics used by the regular subprocess node executor.

#![allow(unsafe_code)] // Win32 Job Object + Unix pre_exec(setsid) need raw FFI

use std::time::Duration;
use tokio::process::{Child, Command};

/// Grace period between SIGTERM and SIGKILL on Unix. Long enough
/// for shells and Python interpreters to flush stdout, short
/// enough that a hung child doesn't block run finalization.
///
/// Windows cancellation uses `TerminateJobObject`, which is hard-
/// kill only — no grace window — so this constant is Unix-only in
/// practice. Defined unconditionally so the [`cancel`] docstring
/// can link to it from any target.
#[cfg_attr(windows, allow(dead_code))]
pub(crate) const CANCEL_GRACE: Duration = Duration::from_secs(2);

// =====================================================================
// Unix
// =====================================================================

/// Owned handle to a spawned child whose process tree is supervised
/// via setsid (Unix) or a Job Object (Windows). Opaque — callers
/// reach the underlying [`Child`] via [`Supervised::child_mut`].
#[cfg(unix)]
pub struct Supervised {
    inner_child: Child,
    pid: u32,
}

#[cfg(unix)]
impl Supervised {
    /// Borrow the inner child mutably so the caller can pipe its
    /// stdio or wait on it. Don't `kill()` it directly — use
    /// [`cancel`] to tear down the whole supervised tree.
    pub const fn child_mut(&mut self) -> &mut Child {
        &mut self.inner_child
    }
}

#[cfg(unix)]
fn configure_unix_command(cmd: &mut Command) {
    // SAFETY: `setsid` is async-signal-safe; only call between fork and exec.
    unsafe {
        cmd.pre_exec(|| {
            nix::unistd::setsid()
                .map(|_| ())
                .map_err(|e| std::io::Error::from_raw_os_error(e as i32))
        });
    }
}

#[cfg(unix)]
fn platform_spawn(mut cmd: Command) -> std::io::Result<Supervised> {
    configure_unix_command(&mut cmd);
    let child = cmd.spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| std::io::Error::other("child has no pid (already reaped)"))?;
    Ok(Supervised {
        inner_child: child,
        pid,
    })
}

#[cfg(unix)]
async fn platform_cancel(sup: &mut Supervised) -> Option<i32> {
    use nix::sys::signal::Signal;

    // Always signal the pgroup — surviving group members (the in-distro
    // sh / curl / wget tree) die even if the main child has already reaped.
    let _ = kill_unix_pgroup(sup.pid, Signal::SIGTERM);

    let grace = tokio::time::sleep(CANCEL_GRACE);
    tokio::select! {
        wait_res = sup.inner_child.wait() => {
            let code = wait_res.ok().and_then(|s| s.code());
            let _ = kill_unix_pgroup(sup.pid, Signal::SIGKILL);
            code
        }
        () = grace => {
            let _ = kill_unix_pgroup(sup.pid, Signal::SIGKILL);
            sup.inner_child.wait().await.ok().and_then(|s| s.code())
        }
    }
}

#[cfg(unix)]
fn kill_unix_pgroup(pid: u32, sig: nix::sys::signal::Signal) -> nix::Result<()> {
    // Real OS PIDs never exceed i32::MAX; saturate defensively so a
    // garbage pid (e.g. already-reaped child with no id) can't wrap.
    let pid_i32 = i32::try_from(pid).unwrap_or(i32::MAX);
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(-pid_i32), sig)
}

// =====================================================================
// Windows
// =====================================================================

/// Owned handle to a spawned child whose process tree is supervised
/// via setsid (Unix) or a Job Object (Windows). Opaque — callers
/// reach the underlying [`Child`] via [`Supervised::child_mut`].
#[cfg(windows)]
pub struct Supervised {
    inner_child: Child,
    job: windows::Win32::Foundation::HANDLE,
}

// SAFETY: Windows kernel HANDLEs are safe to use from any thread —
// concurrency is enforced by the kernel — and we hold this one
// uniquely (no aliasing). HANDLE only fails Send/Sync automatically
// because its inner field is a raw pointer.
#[cfg(windows)]
unsafe impl Send for Supervised {}
#[cfg(windows)]
unsafe impl Sync for Supervised {}

#[cfg(windows)]
impl Drop for Supervised {
    fn drop(&mut self) {
        // SAFETY: closing the job we own. Idempotent on already-closed.
        let close = unsafe { windows::Win32::Foundation::CloseHandle(self.job) };
        drop(close);
    }
}

#[cfg(windows)]
impl Supervised {
    /// Borrow the inner child mutably so the caller can pipe its
    /// stdio or wait on it. Don't `kill()` it directly — use
    /// [`cancel`] to tear down the whole supervised tree.
    pub const fn child_mut(&mut self) -> &mut Child {
        &mut self.inner_child
    }
}

#[cfg(windows)]
fn configure_windows_command(_cmd: &mut Command) {
    // Job-Object setup happens post-spawn in platform_spawn.
}

#[cfg(windows)]
fn platform_spawn(mut cmd: Command) -> std::io::Result<Supervised> {
    use windows::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        SetInformationJobObject,
    };
    use windows::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, CREATE_SUSPENDED};

    configure_windows_command(&mut cmd);
    cmd.creation_flags(CREATE_SUSPENDED.0 | CREATE_NEW_PROCESS_GROUP.0);
    let child = cmd.spawn()?;

    // SAFETY: both args None; HANDLE owned and closed in Supervised::drop.
    let job = unsafe { CreateJobObjectW(None, None) }
        .map_err(|e| std::io::Error::other(format!("CreateJobObjectW: {e}")))?;

    let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    let info_size = u32::try_from(std::mem::size_of_val(&info))
        .expect("JOBOBJECT_EXTENDED_LIMIT_INFORMATION fits in u32");
    let set_res = unsafe {
        SetInformationJobObject(
            job,
            JobObjectExtendedLimitInformation,
            std::ptr::addr_of!(info).cast(),
            info_size,
        )
    };
    if let Err(e) = set_res {
        // SAFETY: closing the job we just created.
        let close = unsafe { windows::Win32::Foundation::CloseHandle(job) };
        drop(close);
        return Err(std::io::Error::other(format!(
            "SetInformationJobObject: {e}"
        )));
    }
    let pid_raw = child
        .id()
        .ok_or_else(|| std::io::Error::other("child has no pid (already reaped)"))?;

    // SAFETY: opening our own child process by pid; closed below.
    let proc_handle = unsafe {
        windows::Win32::System::Threading::OpenProcess(
            windows::Win32::System::Threading::PROCESS_ALL_ACCESS,
            windows::Win32::Foundation::FALSE,
            pid_raw,
        )
    }
    .map_err(|e| std::io::Error::other(format!("OpenProcess: {e}")))?;

    // SAFETY: child_handle stays valid through the AssignProcessToJobObject call.
    let assign_res = unsafe { AssignProcessToJobObject(job, proc_handle) };
    // SAFETY: closing the proc_handle we just opened.
    let close_proc = unsafe { windows::Win32::Foundation::CloseHandle(proc_handle) };
    drop(close_proc);
    if let Err(e) = assign_res {
        // SAFETY: closing the job we just created.
        let close_job = unsafe { windows::Win32::Foundation::CloseHandle(job) };
        drop(close_job);
        return Err(std::io::Error::other(format!(
            "AssignProcessToJobObject: {e}"
        )));
    }

    resume_initial_thread(pid_raw).map_err(|e| std::io::Error::other(format!("resume: {e}")))?;

    Ok(Supervised {
        inner_child: child,
        job,
    })
}

#[cfg(windows)]
fn resume_initial_thread(pid: u32) -> std::io::Result<()> {
    use windows::Win32::Foundation::CloseHandle;
    use windows::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows::Win32::System::Threading::{OpenThread, ResumeThread, THREAD_SUSPEND_RESUME};

    // SAFETY: standard ToolHelp32 traversal.
    unsafe {
        let snap = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0)
            .map_err(|e| std::io::Error::other(format!("snapshot: {e}")))?;
        let mut entry = THREADENTRY32 {
            dwSize: u32::try_from(std::mem::size_of::<THREADENTRY32>())
                .expect("THREADENTRY32 fits in u32"),
            ..Default::default()
        };
        let mut ok = Thread32First(snap, &mut entry).is_ok();
        while ok {
            if entry.th32OwnerProcessID == pid {
                if let Ok(handle) = OpenThread(THREAD_SUSPEND_RESUME, false, entry.th32ThreadID) {
                    // ResumeThread returns the prior suspend count (a u32);
                    // we don't act on it.
                    let _prev = ResumeThread(handle);
                    let close = CloseHandle(handle);
                    drop(close);
                }
            }
            ok = Thread32Next(snap, &mut entry).is_ok();
        }
        let close_snap = CloseHandle(snap);
        drop(close_snap);
        Ok(())
    }
}

#[cfg(windows)]
async fn platform_cancel(sup: &mut Supervised) -> Option<i32> {
    use windows::Win32::System::JobObjects::TerminateJobObject;
    // SAFETY: terminating the job we own. Idempotent on post-exit.
    let terminate = unsafe { TerminateJobObject(sup.job, 1) };
    drop(terminate);
    sup.inner_child.wait().await.ok().and_then(|s| s.code())
}

// =====================================================================
// Cross-platform public API
// =====================================================================

/// Spawn a supervised subprocess.
///
/// Internally configures platform-specific pre-spawn setup
/// (`pre_exec(setsid)` on Unix; `CREATE_SUSPENDED` + Job Object on
/// Windows). On Windows the initial thread is resumed before this
/// returns. Callers don't invoke a separate configure step.
pub fn spawn(cmd: Command) -> std::io::Result<Supervised> {
    platform_spawn(cmd)
}

/// Tear down the supervised tree AND capture the main child's exit
/// code if available. Returns `Some(code)` when the child reaped with
/// an integer status, `None` if killed or unavailable.
///
/// - Unix: always signals the negative pgid (`SIGTERM`,
///   [`CANCEL_GRACE`] wait, `SIGKILL`) regardless of whether the
///   main child has reaped.
/// - Windows: `TerminateJobObject` (idempotent), then reap.
///
/// Callers MUST bind or discard the return value
/// (`let _ = supervisor::cancel(&mut sup).await;`) to discharge the
/// workspace's `unused_must_use = "deny"` lint.
pub async fn cancel(sup: &mut Supervised) -> Option<i32> {
    platform_cancel(sup).await
}
