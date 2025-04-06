use serde::Deserialize;
const PAGE_SIZE: u32 = 100;

pub fn fetch() -> Vec<ResponseHitSource> {
    let mut items = Vec::new();

    for page in 0.. {
        let response = fetch_page(PAGE_SIZE, page);

        println!(
            "fetched\t{} / {}\t{}%",
            page * PAGE_SIZE,
            response.hits.total.value,
            page * PAGE_SIZE * 100 / response.hits.total.value,
        );

        let len = response.hits.hits.len();
        for hit in response.hits.hits {
            items.push(hit.source);
        }

        if len < PAGE_SIZE as usize {
            break;
        }

        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    let prelen = items.len();
    items.sort_by_key(|i: &ResponseHitSource| i.id);
    items.dedup_by_key(|i| i.id);

    println!("deduped {} items", prelen - items.len());

    items
}

fn fetch_page(page_size: u32, page: u32) -> Response {
    let mut response = ureq::get(url(page_size, page))
        .call()
        .expect("request falhou rede");

    assert!(response.status().is_success(), "request falhou status");

    response
        .body_mut()
        .read_json()
        .expect("request falhou corpo")
}

fn url(page_size: u32, page: u32) -> String {
    use std::fmt::Write;
    let offset = page * page_size;
    let mut url = String::from(URL);
    write!(url, "page_size={page_size}&offset={offset}").expect("url");
    url
}

#[derive(Debug, Deserialize)]
struct Response {
    hits: ResponseHits,
}

#[derive(Debug, Deserialize)]
struct ResponseHits {
    total: ResponseHitsTotal,
    hits: Vec<ResponseHit>,
}

#[derive(Debug, Deserialize)]
struct ResponseHitsTotal {
    value: u32,
}

#[derive(Debug, Deserialize)]
struct ResponseHit {
    #[serde(rename = "_source")]
    source: ResponseHitSource,
}

#[derive(Debug, Deserialize, Hash)]
#[serde(rename_all = "camelCase")]
pub struct ResponseHitSource {
    pub id: u32,
    pub rent: u32,
    pub total_cost: u32,
    pub sale_price: u32,
    pub iptu_plus_condominium: u32,
    pub area: Option<u32>,
    pub address: String,
    pub region_name: String,
    pub city: String,
    pub visit_status: String,
    pub r#type: Option<String>,
    pub for_rent: Boolorstr,
    pub for_sale: Option<Boolorstr>,
    pub is_primary_market: Option<bool>,
    pub bedrooms: Option<u8>,
    pub parking_spaces: Option<u8>,
    pub neighbourhood: String,
    pub bathrooms: Option<u8>,
    pub is_furnished: Option<Boolorstr>,
}

#[derive(Debug, Deserialize, Hash)]
#[serde(untagged)]
pub enum Boolorstr {
    Str(Boolstr),
    Bool(bool),
}

impl Boolorstr {
    pub fn to_int(self) -> u8 {
        match self {
            Boolorstr::Bool(true) | Boolorstr::Str(Boolstr::True) => 1,
            Boolorstr::Bool(false) | Boolorstr::Str(Boolstr::False) => 0,
        }
    }
}

#[derive(Debug, Deserialize, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Boolstr {
    False,
    True,
}

const URL: &str = "https://www.quintoandar.com.br/api/yellow-pages/v2/search?\
map[bounds_north]=-19.9121089&\
map[bounds_south]=-19.95602996&\
map[bounds_east]=-43.9149629&\
map[bounds_west]=-43.9758439&\
availability=any&\
occupancy=any&\
business_context=SALE&\
return=id&\
return=coverImage&\
return=rent&\
return=totalCost&\
return=salePrice&\
return=iptuPlusCondominium&\
return=area&\
return=imageList&\
return=imageCaptionList&\
return=address&\
return=regionName&\
return=city&\
return=visitStatus&\
return=activeSpecialConditions&\
return=type&\
return=forRent&\
return=forSale&\
return=isPrimaryMarket&\
return=bedrooms&\
return=parkingSpaces&\
return=listingTags&\
return=yield&\
return=yieldStrategy&\
return=neighbourhood&\
return=categories&\
return=bathrooms&\
return=isFurnished&\
return=installations&";
