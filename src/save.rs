use crate::fetch::ResponseHit;

pub fn save(hits: Vec<ResponseHit>) {
    let mut conn = rusqlite::Connection::open("./db.sqlite").expect("db open");

    conn.execute(
        "CREATE TABLE IF NOT EXISTS imoveis (
            id INTEGER NOT NULL,
            created_at TEXT NOT NULL,

            rent INTEGER NOT NULL,
            total_cost INTEGER NOT NULL,
            sale_price INTEGER NOT NULL,
            iptu_plus_condominium INTEGER NOT NULL,
            area INTEGER,
            address TEXT NOT NULL,
            region_name TEXT NOT NULL,
            city TEXT NOT NULL,
            visit_status TEXT NOT NULL,
            kind TEXT,
            for_rent INTEGER NOT NULL,
            for_sale INTEGER,
            is_primary_market INTEGER,
            bedrooms INTEGER,
            parking_spaces INTEGER,
            neighbourhood TEXT NOT NULL,
            bathrooms INTEGER,
            is_furnished INTEGER
        )",
        (),
    )
    .expect("db setup");

    let tx = conn.transaction().expect("tx create");

    {
        let mut insert = tx
            .prepare(
                "INSERT INTO imoveis (
                    created_at,
                    id,
                    rent,
                    total_cost,
                    sale_price,
                    iptu_plus_condominium,
                    area,
                    address,
                    region_name,
                    city,
                    visit_status,
                    kind,
                    for_rent,
                    for_sale,
                    is_primary_market,
                    bedrooms,
                    parking_spaces,
                    neighbourhood,
                    bathrooms,
                    is_furnished
                ) VALUES (
                    current_timestamp,
                    ?1,
                    ?2,
                    ?3,
                    ?4,
                    ?5,
                    ?6,
                    ?7,
                    ?8,
                    ?9,
                    ?10,
                    ?11,
                    ?12,
                    ?13,
                    ?14,
                    ?15,
                    ?16,
                    ?17,
                    ?18,
                    ?19
                );
            ",
            )
            .expect("db prepare");

        for hit in hits {
            insert
                .execute(rusqlite::params![
                    hit.source.id,
                    hit.source.rent,
                    hit.source.total_cost,
                    hit.source.sale_price,
                    hit.source.iptu_plus_condominium,
                    hit.source.area,
                    hit.source.address,
                    hit.source.region_name,
                    hit.source.city,
                    format!("{:?}", hit.source.visit_status),
                    hit.source.kind.map(|kind| format!("{kind:?}")),
                    hit.source.for_rent.to_int(),
                    hit.source.for_sale.map(|a| a.to_int()),
                    hit.source.is_primary_market,
                    hit.source.bedrooms,
                    hit.source.parking_spaces,
                    hit.source.neighbourhood,
                    hit.source.bathrooms,
                    hit.source.is_furnished.map(|a| a.to_int()),
                ])
                .expect("insert");
        }
    }

    tx.commit().expect("tx commit");
}
