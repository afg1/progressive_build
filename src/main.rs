use clap::Parser;
use std::fs::File;
use std::path::PathBuf;
use anyhow::Result;
use std::time::Instant;

use polars::prelude::*;




#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// List of input files. Ideally with headers
    #[clap(short, long, multiple_values(true))]
    input: Vec<PathBuf>,

    /// An output file
    #[clap(short, long)]
    output: String,

    /// The column to select. If files have no headers, use "column_N"
    #[clap(short, long)]
    select: String,
}


fn load_data(path: &PathBuf, delim: u8) -> Result<DataFrame> {
    let df:DataFrame = CsvReader::from_path(path)?
    .has_header(true)
    .with_delimiter(delim)
    .finish().unwrap();
    Ok(df)
}



fn main()  {
    let timer = Instant::now();

    let mut args = Args::parse();

    let mut n_files = 0;
    let mut output: Option<DataFrame> = None;

    while let Some(infile) = args.input.pop() {
    // Load the input Csv
        let mut input:DataFrame = if infile.extension().unwrap() == "tsv"
        {
            load_data(&infile, b'\t')
        }
        else
        {
            load_data(&infile, b',')
        }.unwrap_or_else(|error| {
            match error.downcast_ref::<PolarsError>()
            {
                Some(PolarsError::Io(_string)) => panic!("Input file does not exist! {:?}", args.input),
                _ => panic!("An error occurred! {:?}", error),

            }
        });

        // Rename columns to remove . in the names
        let mut new_cols = Vec::new();
         for nm in input.get_column_names().iter() {
            new_cols.push(nm.replace(".", ""));
        };

        if new_cols != input.get_column_names() {
            input.set_column_names(&new_cols).unwrap_or_else(|error| {
                panic!("Failed to set column names for some reason {:?}", error);
            });
        }
        n_files += 1;


        // Drop everything from the input except the genes
        let genes:DataFrame = input.select([&args.select]).unwrap_or_else(|error| {
            match error
            {
                PolarsError::NotFound(_string) => {panic!("{} was not found in the header, does the file have a header?\n{:?}", args.select, input.get_column_names());},
                _ => panic!("Error selecting column from input: {:?}", error),

            }
        });

        output = match output
        {
            None => {
                println!("Writethrough");
                let output_df = genes.clone();
                Some(output_df)
            },
            Some(mut output_df) => {
            // genes =/= output, so we are handling a new file
                output_df.extend(&genes).unwrap_or_else(|error| {
                    match error {
                        _ => panic!("Failed to extend the output dataframe. {:?}", error),
                    }
                });
                output_df = output_df.unique(None, UniqueKeepStrategy::First).unwrap_or_else(|error| {
                    match error {
                        _ => panic!("Failed to parse selected column for uniqueness. {:?}", error),
                    }
                });
                Some(output_df)
                }
        }
    }
    //
    match output {
        None =>  println!("Nothing was processed, or no output generated"),
        Some(mut output_df) => {
            let out_stream : File = File::create(args.output).unwrap();
            CsvWriter::new(out_stream)
            .has_header(true)
            .finish(&mut output_df)
            .unwrap_or_else(|error|{
                match error {
                    _ => panic!("Something wrong writing file {:?}", error),
                }
            });

            println!("Processed {} in {} seconds", n_files, timer.elapsed().as_secs());
        }

    }

}
