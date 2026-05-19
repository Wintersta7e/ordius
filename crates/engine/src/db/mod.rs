//! Database access — `r2d2` connection pool + schema migrations.

mod migrations;

use crate::Result;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use std::path::Path;

/// `r2d2` connection pool over a `SQLite` database.
pub type DbPool = Pool<SqliteConnectionManager>;

/// Open (or create) the `SQLite` run database at `path`.
///
/// Applies any pending migrations to bring the database up to the
/// current schema version. WAL journalling and foreign-key
/// enforcement are enabled on every connection.
pub fn open<P: AsRef<Path>>(path: P) -> Result<DbPool> {
    let mgr = SqliteConnectionManager::file(path)
        .with_init(|c| c.execute_batch("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;"));
    let pool = Pool::builder().max_size(8).build(mgr)?;
    let conn = pool.get()?;
    migrations::apply(&conn)?;
    Ok(pool)
}

#[cfg(test)]
mod tests;
