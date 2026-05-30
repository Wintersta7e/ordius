//! Gated russh API spike for Phase G.
//!
//! This example exists to confirm exact russh/russh-sftp APIs before the real
//! dispatcher code depends on them. It exits immediately unless
//! `ORDIUS_SSH_SPIKE=1` and `ORDIUS_TEST_SSH_HOST=user@box` are set.

use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::time::Duration;

use tokio::io::AsyncWriteExt as _;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    // Enable tracing for diagnostic output when RUST_LOG is set.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init()
        .ok();

    if std::env::var("ORDIUS_SSH_SPIKE").ok().as_deref() != Some("1") {
        eprintln!("set ORDIUS_SSH_SPIKE=1 to run the real SSH spike");
        return Ok(());
    }

    let target = std::env::var("ORDIUS_TEST_SSH_HOST").ok().ok_or_else(|| {
        anyhow::anyhow!("ORDIUS_TEST_SSH_HOST must be user@host or user@host:port")
    })?;
    let password = std::env::var("ORDIUS_TEST_SSH_PASSWORD").ok();
    let key_path = std::env::var("ORDIUS_TEST_SSH_KEY")
        .ok()
        .map(PathBuf::from)
        .or_else(default_key_path);

    let parsed = Target::parse(&target)?;
    let addr = format!("{}:{}", parsed.host, parsed.port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow::anyhow!("no address resolved for {}", parsed.host))?;

    // All russh calls below are confirmed against russh 0.61.1.
    // Keep accepted signatures here; they are copied into Spike Findings.
    let config = russh::client::Config {
        inactivity_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    };
    let config = std::sync::Arc::new(config);
    // connect() returns Handle<H> which is the mutable session object.
    let mut session = russh::client::connect(config, addr, SpikeHandler).await?;

    if let Some(password) = password {
        // authenticate_password: takes (&str, impl Into<String>).
        let auth = session
            .authenticate_password(&parsed.user, password)
            .await?;
        println!("password auth result: {auth:?}");
    } else {
        let key_path = key_path.ok_or_else(|| anyhow::anyhow!("missing ORDIUS_TEST_SSH_KEY"))?;
        // load_secret_key: returns Result<PrivateKey, keys::Error>.
        let private_key = russh::keys::load_secret_key(&key_path, None)?;
        // authenticate_publickey: takes (user, PrivateKeyWithHashAlg) by value.
        // NOT authenticate_publickey_with (that is for external signers/SSH agent).
        let key = russh::keys::PrivateKeyWithHashAlg::new(
            std::sync::Arc::new(private_key),
            None, // hash_alg; None maps to sha-rsa for RSA, ignored for Ed25519
        );
        let auth = session.authenticate_publickey(&parsed.user, key).await?;
        println!("key auth result: {auth:?}");
    }

    // Exec channel: channel_open_session() -> Channel<client::Msg>.
    // exec(want_reply: bool, command: impl Into<&[u8]>).
    // channel.wait() -> Option<ChannelMsg> drives the message loop.
    let mut channel = session.channel_open_session().await?;
    channel.exec(true, "printf 'ordius-spike\\n'").await?;
    while let Some(msg) = channel.wait().await {
        println!("exec message: {msg:?}");
    }

    // Direct-tcpip: channel_open_direct_tcpip(host, port, orig_addr, orig_port).
    // The OPEN must succeed — a real open failure would mean T11's HTTP tunnel
    // can't work either, so keep `?` here.
    let mut direct = session
        .channel_open_direct_tcpip("127.0.0.1", 22, "127.0.0.1", 0)
        .await?;
    // Best-effort read of one message from the forwarded socket with a short
    // timeout. Forwarding to 127.0.0.1:22 (the container's own sshd) yields
    // the remote sshd's SSH identification banner immediately, which proves the
    // channel carries data end-to-end. If the timeout fires or there is nothing
    // to read we print that and continue — this is not a failure.
    match tokio::time::timeout(Duration::from_secs(2), direct.wait()).await {
        Ok(Some(russh::ChannelMsg::Data { ref data })) => {
            let snippet = String::from_utf8_lossy(data);
            let n = data.len();
            println!(
                "direct-tcpip carried {n} bytes: {:?}",
                &snippet[..snippet.len().min(60)]
            );
        },
        Ok(Some(other)) => {
            println!("direct-tcpip first message: {other:?}");
        },
        Ok(None) => {
            println!("direct-tcpip opened; channel closed with no data");
        },
        Err(_) => {
            println!("direct-tcpip opened; no data within 2 s timeout");
        },
    }
    // Drain remaining messages and tear down cleanly before opening SFTP.
    // Send our EOF+Close first (tolerate errors — channel may already be closing).
    let _eof = direct.eof().await;
    let _close = direct.close().await;
    // Drain until wait() returns None (fully closed), with a 5 s safety cap.
    // Unread ChannelClose messages from the direct-tcpip teardown must be
    // consumed before opening the SFTP channel to avoid confusing the session.
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        while direct.wait().await.is_some() {}
    })
    .await;

    // SFTP: SftpSession::new requires AsyncRead + AsyncWrite + Unpin + Send + 'static.
    // Channel<client::Msg> does NOT implement those directly; use .into_stream()
    // to get ChannelStream<client::Msg>, which does.
    let sftp_channel = session.channel_open_session().await?;
    // request_subsystem must be called before handing the channel to SftpSession.
    // into_stream() converts Channel<Msg> -> ChannelStream<Msg> (AsyncRead+AsyncWrite).
    sftp_channel.request_subsystem(true, "sftp").await?;
    let sftp = russh_sftp::client::SftpSession::new(sftp_channel.into_stream()).await?;

    // canonicalize returns String (not PathBuf); build paths via string concat.
    let home = sftp.canonicalize(".").await?;
    let tmp = format!("{home}/.ordius-spike.tmp");
    let final_path = format!("{home}/.ordius-spike");
    // create() opens/truncates, returns File (implements AsyncWrite).
    let mut file = sftp.create(&tmp).await?;
    file.write_all(b"ordius-spike\n").await?;
    // shutdown() flushes + sends FXP_CLOSE (preferred over drop which is fire-and-forget).
    file.shutdown().await?;
    // SSH_FXP_RENAME (SFTP v3) refuses to overwrite an existing destination.
    // Remove the target first so the rename is idempotent across spike re-runs.
    drop(sftp.remove_file(&final_path).await); // best-effort; ignore "no such file"
    sftp.rename(&tmp, &final_path).await?;
    println!("sftp wrote {final_path}");
    // Close SFTP session cleanly before dropping the SSH session.
    sftp.close().await?;

    // Disconnect the SSH session gracefully.
    session
        .disconnect(russh::Disconnect::ByApplication, "done", "en")
        .await?;

    Ok(())
}

fn default_key_path() -> Option<PathBuf> {
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"))?;
    let home = PathBuf::from(home);
    let ed25519 = home.join(".ssh").join("id_ed25519");
    ed25519.exists().then_some(ed25519)
}

struct Target {
    user: String,
    host: String,
    port: u16,
}

impl Target {
    fn parse(raw: &str) -> anyhow::Result<Self> {
        let (user, host_port) = raw
            .split_once('@')
            .ok_or_else(|| anyhow::anyhow!("ORDIUS_TEST_SSH_HOST must be user@host"))?;
        let (host, port) = match host_port.rsplit_once(':') {
            Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) => (h.to_string(), p.parse()?),
            _ => (host_port.to_string(), 22),
        };
        Ok(Self {
            user: user.to_string(),
            host,
            port,
        })
    }
}

struct SpikeHandler;

impl russh::client::Handler for SpikeHandler {
    type Error = russh::Error;

    // check_server_key: &mut self, &russh::keys::ssh_key::PublicKey -> Result<bool, Self::Error>
    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::ssh_key::PublicKey,
    ) -> Result<bool, Self::Error> {
        println!(
            "server key: {}",
            server_public_key.fingerprint(russh::keys::ssh_key::HashAlg::default())
        );
        Ok(true)
    }
}
