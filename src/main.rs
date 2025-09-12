use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut header = [0; 108];
            file.read_exact(&mut header)?;

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            #[allow(unused_variables)]
            let page_size = u16::from_be_bytes([header[16], header[17]]);
            let table_count = u16::from_be_bytes([header[103], header[104]]);

            // You can use print statements as follows for debugging, they'll be visible when running tests.
            eprintln!("Logs from your program will appear here!");

            // Uncomment this block to pass the first stage
            println!("database page size: {}", page_size);
            println!("number of tables: {}", table_count);
        }
        ".tables" => {
            let mut file = File::open(&args[1])?;
            let mut header = [0; 108];
            file.read_exact(&mut header)?;
            let table_count = u16::from_be_bytes([header[103], header[104]]);

            // the cells of the first page contain information on the tables
            // each cell has a row
            // each row has a tbl_name value which has the table's name
            // the cell pointer array starts right after the header
            let mut cell_pointer_array: Vec<u8> = vec![0; table_count as usize * 2];
            file.read_exact(&mut cell_pointer_array)?;

            for i in (0..table_count * 2).step_by(2) {
                let cell_pointer = u16::from_be_bytes([
                    cell_pointer_array[i as usize],
                    cell_pointer_array[i as usize + 1],
                ]);
                // On page 1, cell pointers are offsets from the start of the page (file offset 0)
                // Do NOT add 100 here. The 100-byte database header is accounted for in the offsets.
                file.seek(std::io::SeekFrom::Start(cell_pointer as u64))?;

                let mut payload_size: u64 = 0;
                let mut row_id: u64 = 0;
                let mut bytes_read = 0;
                // read payload size
                loop {
                    let mut b = [0u8; 1];
                    file.read_exact(&mut b)?;
                    payload_size = (payload_size << 7) | u64::from(b[0] & 0x7F);
                    bytes_read += 1;
                    if (b[0] & 0x80) == 0 {
                        break;
                    }

                    if bytes_read == 8 {
                        file.read_exact(&mut b)?;
                        payload_size = (payload_size << 8) | u64::from(b[0]);
                        break;
                    }
                }

                bytes_read = 0;
                // read row id
                loop {
                    let mut b = [0u8; 1];
                    file.read_exact(&mut b)?;
                    row_id = (row_id << 7) | u64::from(b[0] & 0x7F);
                    bytes_read += 1;
                    if (b[0] & 0x80) == 0 {
                        break;
                    }

                    if bytes_read == 8 {
                        file.read_exact(&mut b)?;
                        row_id = (row_id << 8) | u64::from(b[0]);
                        break;
                    }
                }

                let mut payload: Vec<u8> = vec![0; payload_size as usize];
                file.read_exact(&mut payload)?;

                // this is our cursor to work in the payload buffer
                let mut p = 0;
                // decode header size
                let mut header_size: u64 = 0;
                bytes_read = 0;
                loop {
                    let b = payload[p];
                    p += 1;

                    header_size = (header_size << 7) | u64::from(b & 0x7F);
                    bytes_read += 1;

                    // stop when MSB == 0
                    if (b & 0x80) == 0 {
                        break;
                    }

                    // 9th-byte special case
                    if bytes_read == 8 {
                        let b9 = payload[p];
                        p += 1;
                        header_size = (header_size << 8) | u64::from(b9);
                        bytes_read += 1; // add this
                        break;
                    }
                }
                // Sanity: header_size must fit within payload
                if (header_size as usize) > payload.len() {
                    // malformed row, skip
                    continue;
                }

                // Mark the start of header bytes (immediately after header_size varint)
                let header_start = p;
                let mut header_bytes_remaining = (header_size as usize) - bytes_read;

                let mut serial_types: Vec<u64> = Vec::new();
                // sqlite_schema has 5 columns. Decode exactly up to 5 serial types from header bytes
                for _ in 0..5 {
                    // start a new serial type varint
                    let mut st: u64 = 0;
                    let mut st_bytes: usize = 0;

                    // stop if we ran out of header bytes
                    if header_bytes_remaining == 0 || p >= payload.len() {
                        break;
                    }

                    // read 1–9 bytes of this serial type
                    loop {
                        // bounds: don't read beyond header area or payload
                        if st_bytes >= header_bytes_remaining || p >= payload.len() {
                            break;
                        }

                        let b = payload[p];
                        p += 1;

                        st = (st << 7) | u64::from(b & 0x7F);
                        st_bytes += 1;

                        // stop when MSB == 0
                        if (b & 0x80) == 0 {
                            break;
                        }

                        // 9th-byte special case
                        if st_bytes == 8 {
                            if header_bytes_remaining > st_bytes && p < payload.len() {
                                let b9 = payload[p];
                                p += 1;
                                st = (st << 8) | u64::from(b9);
                                st_bytes += 1;
                            }
                            break;
                        }
                    }

                    if st_bytes == 0 {
                        // No more header bytes available — avoid infinite loop
                        break;
                    }

                    // store this serial type and account for header bytes consumed
                    serial_types.push(st);
                    header_bytes_remaining = header_bytes_remaining.saturating_sub(st_bytes);
                }
                // Data area begins at offset `header_size` from the start of the record (payload buffer)
                let mut q = header_size as usize;
                if q > payload.len() {
                    // malformed row, skip
                    continue;
                }
                let mut type_str: Option<String> = None;

                for (i, &st) in serial_types.iter().enumerate() {
                    // we’ll decode one column value based on `st`, advancing `q` as we read
                    let col_len: usize = match st {
                        0 => 0,                                                // NULL
                        1 => 1,                                                // 1-byte int
                        2 => 2,                                                // 2-byte int
                        3 => 3,                                                // 3-byte int
                        4 => 4,                                                // 4-byte int
                        5 => 6,                                                // 6-byte int
                        6 => 8,                                                // 8-byte int
                        7 => 8,                                                // 8-byte float
                        8 | 9 => 0, // integer 0 / 1 (encoded with no payload bytes)
                        s if s >= 12 && s % 2 == 0 => ((s - 12) / 2) as usize, // BLOB
                        s if s >= 13 && s % 2 == 1 => ((s - 13) / 2) as usize, // TEXT
                        _ => 0,
                    };

                    // bounds guard for data area
                    if q + col_len > payload.len() {
                        break; // or continue, but breaking this row is safest
                    }

                    let col_bytes = &payload[q..q + col_len];
                    q += col_len;

                    // capture "type" column (i == 0) if TEXT
                    if i == 0 && st >= 13 && st % 2 == 1 {
                        type_str = Some(String::from_utf8_lossy(col_bytes).to_string());
                    }

                    // print tbl_name (i == 2) only if type == "table"
                    if i == 2 && st >= 13 && st % 2 == 1 {
                        if matches!(type_str.as_deref(), Some("table")) {
                            let tbl_name = String::from_utf8_lossy(col_bytes).to_string();
                            println!("{}", tbl_name);
                        }
                    }
                }
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
