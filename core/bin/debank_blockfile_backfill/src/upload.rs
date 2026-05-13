//! S3 upload and local file write for assembled `DebankOutPut`.
//!
//! S3 layout matches EN realtime (`debank_s3_persistence::upload_to_s3`):
//! - Header     -> chaintable-nodex-pipeline--apne1-az4--x-s3 at {chain}/[{version}/]{blockHash}/block
//! - BlockFile  -> chaintable-pipeline--apne1-az4--x-s3       at {chain}/[{version}/]{blockHash}
//! - Validation -> chaintable-pipeline--apne1-az4--x-s3       at {chain}/[{version}/]{blockNum}/{blockHash}

use std::path::Path;

use aws_sdk_s3::Client as S3Client;
use flate2::{write::GzEncoder, Compression};
use zksync_types::debank::DebankOutPut;

const HEADER_BUCKET: &str = "chaintable-nodex-pipeline--apne1-az4--x-s3";
const BLOCK_FILE_BUCKET: &str = "chaintable-pipeline--apne1-az4--x-s3";

fn prefix(chain_id: u64, version: Option<&str>) -> String {
    match version {
        Some(v) if !v.is_empty() => format!("{}/{}", chain_id, v),
        _ => chain_id.to_string(),
    }
}

fn gzip_json<T: serde::Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    serde_json::to_writer(&mut gz, value)?;
    Ok(gz.finish()?)
}

/// Upload header, block_file, and validation files to S3.
pub async fn upload_to_s3(
    s3: &S3Client,
    chain_id: u64,
    version: Option<&str>,
    output: &DebankOutPut,
) -> anyhow::Result<()> {
    let block_hash = format!("{:#x}", output.header.hash);
    let block_num = output.header.number;
    let prefix = prefix(chain_id, version);

    // 1. Header -> chaintable-nodex-pipeline bucket at {prefix}/{blockHash}/block
    let header_key = format!("{}/{}/block", prefix, block_hash);
    let header_body = gzip_json(&output.header)?;
    s3.put_object()
        .bucket(HEADER_BUCKET)
        .key(&header_key)
        .body(header_body.into())
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("upload header for block {}: {:?}", block_num, e))?;

    // 2. BlockFile -> chaintable-pipeline bucket at {prefix}/{blockHash}
    let block_file_key = format!("{}/{}", prefix, block_hash);
    let block_file_body = gzip_json(&output.block_file)?;
    s3.put_object()
        .bucket(BLOCK_FILE_BUCKET)
        .key(&block_file_key)
        .body(block_file_body.into())
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("upload block_file for block {}: {:?}", block_num, e))?;

    // 3. Validation -> chaintable-pipeline bucket at {prefix}/{blockNum}/{blockHash}
    let validation = output.block_file.validation();
    let validation_key = format!("{}/{}/{}", prefix, block_num, block_hash);
    let validation_body = gzip_json(&validation)?;
    s3.put_object()
        .bucket(BLOCK_FILE_BUCKET)
        .key(&validation_key)
        .body(validation_body.into())
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("upload validation for block {}: {:?}", block_num, e))?;

    Ok(())
}

/// Write header, block_file, and validation as gzipped JSON files under
/// `<dir>/<chain_id>/[<version>/]{header,blockfile,validation}/...`.
///
/// The on-disk layout intentionally diverges from S3 (which uses overlapping
/// key prefixes: `{hash}` for BlockFile and `{hash}/block` for header — fine on
/// S3, impossible on a POSIX filesystem). Each kind gets its own subdir, so
/// diffing against S3 baseline only requires gunzipping the matching file.
pub fn write_local(
    dir: &Path,
    chain_id: u64,
    version: Option<&str>,
    output: &DebankOutPut,
) -> anyhow::Result<()> {
    use std::fs;

    let block_hash = format!("{:#x}", output.header.hash);
    let block_num = output.header.number;
    let prefix = prefix(chain_id, version);
    let base = dir.join(&prefix);

    let header_dir = base.join("header");
    fs::create_dir_all(&header_dir)?;
    fs::write(header_dir.join(&block_hash), gzip_json(&output.header)?)?;

    let blockfile_dir = base.join("blockfile");
    fs::create_dir_all(&blockfile_dir)?;
    fs::write(
        blockfile_dir.join(&block_hash),
        gzip_json(&output.block_file)?,
    )?;

    let validation_dir = base.join("validation").join(block_num.to_string());
    fs::create_dir_all(&validation_dir)?;
    fs::write(
        validation_dir.join(&block_hash),
        gzip_json(&output.block_file.validation())?,
    )?;

    Ok(())
}
