//! Prints the GraphQL schema. `just schema` writes it to `schemas/native.graphql`.

fn main() {
    print!("{}", datalens_api::graphql_sdl());
}
