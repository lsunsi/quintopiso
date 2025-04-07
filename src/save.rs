use std::hash::Hasher;

pub fn save(hits: Vec<crate::fetch::ResponseHitSource>, path: &str) {
    use rusqlite::OptionalExtension;
    use std::hash::Hash;

    let mut conn = rusqlite::Connection::open(path).expect("db open");
    conn.execute(CREATE_TABLE, ()).expect("db create");

    let mut tx = conn.transaction().expect("tx create");
    tx.set_drop_behavior(rusqlite::DropBehavior::Commit);

    tx.execute(INACTIVATE_ALL, ()).expect("db setup");
    let mut insert = tx.prepare(INSERT_ACTIVE).expect("db prepare");

    let mut already_known = 0;
    let mut new_version = 0;
    let mut new_entry = 0;

    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("ts")
        .as_secs();

    for hit in hits {
        let mut hasher = std::hash::DefaultHasher::new();
        hit.hash(&mut hasher);
        let hash = i64::from_ne_bytes(hasher.finish().to_ne_bytes());

        let (current_hash, current_version): (Option<i64>, Option<u32>) = tx
            .query_row(FIND_LATEST, (hit.id,), |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()
            .expect("db find")
            .unzip();

        if current_hash.is_some_and(|h| hash == h) {
            already_known += 1;
            tx.execute(UPDATE_SEEN, (hit.id,)).expect("db update");
        } else {
            let version = match current_version {
                Some(v) => {
                    new_version += 1;
                    v + 1
                }
                None => {
                    new_entry += 1;
                    0
                }
            };

            insert
                .execute(rusqlite::params![
                    hit.id,
                    hash,
                    version,
                    1,
                    ts,
                    ts,
                    hit.rent,
                    hit.total_cost,
                    hit.sale_price,
                    hit.iptu_plus_condominium,
                    hit.area,
                    hit.address,
                    hit.region_name,
                    hit.city,
                    hit.visit_status,
                    hit.r#type,
                    hit.for_rent.to_int(),
                    hit.for_sale.map(|a| a.to_int()),
                    hit.is_primary_market,
                    hit.bedrooms,
                    hit.parking_spaces,
                    hit.neighbourhood,
                    hit.bathrooms,
                    hit.is_furnished.map(|a| a.to_int()),
                ])
                .expect("insert");
        }
    }

    println!("saved already known = {already_known}");
    println!("saved new versions = {new_version}");
    println!("saved new entries = {new_entry}");
}

const CREATE_TABLE: &str = "
CREATE TABLE IF NOT EXISTS imoveis (
    id INTEGER NOT NULL,
    hash INTEGER NOT NULL,
    version INTEGER NOT NULL,
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    created_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,

    rent INTEGER NOT NULL CHECK (rent >= 0),
    total_cost INTEGER NOT NULL CHECK (total_cost >= 0),
    sale_price INTEGER NOT NULL CHECK (sale_price >= 0),
    iptu_plus_condominium INTEGER NOT NULL CHECK (iptu_plus_condominium >= 0),
    area INTEGER CHECK (area >= 0),
    address TEXT NOT NULL CHECK (length(address) >= 0),
    region_name TEXT NOT NULL CHECK (length(region_name) >= 0),
    city TEXT NOT NULL CHECK (length(city) >= 0),
    visit_status TEXT NOT NULL CHECK (length(visit_status) >= 0),
    type TEXT CHECK (length(type) >= 0),
    for_rent INTEGER NOT NULL CHECK (for_rent IN (0, 1)),
    for_sale INTEGER CHECK (for_sale IN (0, 1)),
    is_primary_market INTEGER CHECK (is_primary_market IN (0, 1)),
    bedrooms INTEGER CHECK (bedrooms >= 0),
    parking_spaces INTEGER CHECK (parking_spaces >= 0),
    neighbourhood TEXT NOT NULL  CHECK (length(neighbourhood) >= 0),
    bathrooms INTEGER CHECK (bathrooms >= 0),
    is_furnished INTEGER CHECK (is_furnished IN (0, 1))
);";

const INACTIVATE_ALL: &str = "
UPDATE imoveis
SET active = 0
WHERE active = 1;";

const FIND_LATEST: &str = "
SELECT hash, version
FROM imoveis
WHERE id = ?1
ORDER BY version DESC;";

const UPDATE_SEEN: &str = "
UPDATE imoveis
SET active = 1, last_seen_at = CURRENT_TIMESTAMP
WHERE id = ?1;";

const INSERT_ACTIVE: &str = "
INSERT INTO imoveis (
    id,
    hash,
    version,
    active,
    created_at,
    last_seen_at,
    rent,
    total_cost,
    sale_price,
    iptu_plus_condominium,
    area,
    address,
    region_name,
    city,
    visit_status,
    type,
    for_rent,
    for_sale,
    is_primary_market,
    bedrooms,
    parking_spaces,
    neighbourhood,
    bathrooms,
    is_furnished
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24);";
