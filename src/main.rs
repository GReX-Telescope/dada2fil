use clap::Parser;
use hifitime::prelude::*;
use psrdada::builder::DadaClientBuilder;
use sigproc_filterbank::write::WriteFilterbank;
use std::{fs::File, io::Write, str::FromStr};

#[derive(Parser, Debug)]
struct Args {
    /// Hex key for PSRDADA connection
    #[clap(short, value_parser = valid_dada_key)]
    key: i32,
    /// Filename to save data to
    #[clap(short)]
    filename: String,
}

fn valid_dada_key(s: &str) -> Result<i32, String> {
    i32::from_str_radix(s, 16).map_err(|_| "Invalid hex litteral".to_string())
}

fn main() {
    let args = Args::parse();
    let mut client = DadaClientBuilder::new(args.key).build().unwrap();
    let (mut header_client, mut data_client) = client.split();
    // Read one frame from the header to get the filterbank metadata
    let metadata = header_client.pop_header().unwrap();
    let freq: f64 = metadata
        .get("FREQ")
        .expect("Header missing FREQ")
        .parse()
        .expect("Not a float");
    let bw: f64 = metadata
        .get("BW")
        .expect("Header missing BW")
        .parse()
        .expect("Not a float");
    let nchan: usize = metadata
        .get("NCHAN")
        .expect("Header missing NCHAN")
        .parse()
        .expect("Not an integer");
    let nbit: u32 = metadata
        .get("NBIT")
        .expect("Header missing NBIT")
        .parse()
        .expect("Not an integer");
    assert_eq!(nbit, 32, "Only f32 data is supported");
    let tsamp: f64 = metadata
        .get("TSAMP")
        .expect("Header missing TSAMP")
        .parse()
        .expect("Not an integer");
    let utc_start_str = metadata.get("UTC_START").expect("Header missing UTC_START");
    let fmt = Format::from_str("%Y-%m-%d-%H:%M:%S").unwrap();
    let utc_start = Epoch::from_str_with_format(utc_start_str, fmt).expect("Not a timestamp");
    // Compute the data needed for the filterbank file header
    let foff = -bw / nchan as f64;
    let tsamp = tsamp / 1e6; // Heimdall wants us, sigproc wants s
    let fch1 = freq + bw / 2.0 - bw / nchan as f64;
    // Setup the filterbank file
    let mut file = File::create(args.filename).expect("Could not create file");
    // We need, fch1, foff, tsamp
    let mut fb = WriteFilterbank::<f32>::new(nchan, 1);
    // Setup the headers
    fb.fch1 = Some(fch1);
    fb.foff = Some(foff);
    fb.tsamp = Some(tsamp);
    fb.tstart = Some(utc_start.to_mjd_utc_days());
    // Write the header
    file.write_all(&fb.header_bytes()).unwrap();
    // Stream in the data forever
    loop {
        // Read all the bytes from this block
        if let Some(bytes) = data_client.reader().pop() {
            // This vector of bytes is [[CH0 CH1 CH2 ... CH(nchan-1)] [CH0 CH1 CH2 ... CH(nchan-1)]] for N samples
            // 4 * nchan bytes per time sample
            for chunk in bytes.chunks_exact(4 * nchan) {
                // Reinterpret this as an array of float32s
                let ptr = chunk.as_ptr() as *const f32;
                let floats: &[f32] = unsafe { std::slice::from_raw_parts(ptr, nchan) };
                // And write to the file
                file.write_all(&fb.pack(floats)).unwrap();
            }
        } else {
            eprintln!("PSRDADA signalled end of data");
            break;
        }
    }
}
