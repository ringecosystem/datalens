use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "datalens-chain-cache")]
struct Cli {}

fn main() {
    let _cli = Cli::parse();
}
