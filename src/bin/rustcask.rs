use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
    },

    #[clap(name = "rm")]
    Remove {
        key: String,
    },
}

fn main() {
    let args = Args::parse();
    match args.command {
        Commands::Get { key } => {
            panic!("unimplemented");
        }
        Commands::Set { key, value } => {
            panic!("unimplemented");
        }
        Commands::Remove { key } => {
            panic!("unimplemented");
        }
    }
}
