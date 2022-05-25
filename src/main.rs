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
    #[clap(short, long, multiple_values(true))]
    select: Vec<String>,

    /// The size of chunks to process at once
    #[clap(short, long, default_value_t=100)]
    chunksize: usize
}


fn load_data(path: &PathBuf, delim: u8) -> Result<DataFrame> {
    let mut df:DataFrame = CsvReader::from_path(path)?
    .has_header(true)
    .with_delimiter(delim)
    .with_ignore_parser_errors(true)
    .finish().unwrap();

    // hstack the experiment name (derived from the filename) into the DataFrame
    let iter_exp = vec![path.file_name().unwrap().to_str().unwrap().replace("-transcripts", "").replace("-tpms.tsv", "")].into_iter();
    let mut exp_col: Series = iter_exp.flat_map(|n| std::iter::repeat(n).take(df.height())).into_iter().collect();
    exp_col.rename("experiment");
    let full_df:DataFrame = df.with_column(exp_col).unwrap().clone();

    Ok(full_df)
}


fn load_chunk(paths:&mut Vec<PathBuf>, select:&Vec<String>) -> Result<DataFrame, anyhow::Error>
{
    /*
    Work with the chunks of input to reduce them into a single dataframe of the genes we want from
    that particular set of experiments. Should return a single dataframe
    */
    let mut output: Option<DataFrame> = None;
    while let Some(infile) = paths.pop() {
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
                Some(PolarsError::Io(_string)) => panic!("Input file does not exist! {:?}", infile),
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

        // Drop everything from the input except the genes
        let genes:DataFrame = input.select(select.iter()).unwrap_or_else(|error| {
            match error
            {
                PolarsError::NotFound(_string) => {panic!("{:?} was not found in the header, does the file have a header?\n{:?}", select, input.get_column_names());},
                _ => panic!("Error selecting column from input: {:?}", error),

            }
        });

        output = match output
        {
            None => {
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
    return Ok(output.unwrap())
}


fn main()  {
    let timer = Instant::now();

    let mut args = Args::parse();

    let  n_files = args.input.len();

    let mut dataframes: Vec<DataFrame> = Vec::new();
    let  chunked_input: Vec<Vec<PathBuf>> = args.input.chunks(args.chunksize).map(|x| x.to_vec()).collect();
    // Read everything into a big vector
    let mut n_chunks = 0;
    for mut files_chunk in chunked_input
    {
        dataframes.push( load_chunk(&mut files_chunk, &args.select).unwrap_or_else(|error| {
            panic!("Something wrong in one of the reads, aborting")
        }) );
        n_chunks += 1;
        println!("Done {} files", n_chunks * args.chunksize);
    }


    let mut output:DataFrame = dataframes[0].clone();

    dataframes.iter()
                .map(|item| {
                     output.extend(&item);
                     output.unique(None, UniqueKeepStrategy::First).unwrap()
                 });

    //
    // match output {
    //     None =>  println!("Nothing was processed, or no output generated"),
    //     Some(mut output_df) => {
            let out_stream : File = File::create(args.output).unwrap();
            CsvWriter::new(out_stream)
            .has_header(true)
            .finish(&mut output)
            .unwrap_or_else(|error|{
                match error {
                    _ => panic!("Something wrong writing file {:?}", error),
                }
            });
    //
    println!("Processed {} in {} seconds", n_files, timer.elapsed().as_secs());
    //     }
    //
    // }

}
