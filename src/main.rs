mod fetch;
mod save;

fn main() {
    let response = fetch::fetch();
    println!("fetched len={}", response.len());

    save::save(response, "./db.sqlite");
}
