use serde::Deserialize;
use std::collections::HashMap;

pub fn fetch() -> Vec<ResponseHit> {
    let mut hits = HashMap::new();

    for page in 0.. {
        dbg!(page);

        let response = fetch_page(page);
        let len = response.hits.hits.len();

        for hit in response.hits.hits {
            if hits.insert(hit.source.id, hit).is_some() {
                println!("se liga tá deduplicando");
            }
        }

        if len < 1000 {
            break;
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    hits.into_values().collect()
}

fn fetch_page(page: u32) -> Response {
    let mut response = ureq::get(String::from(URL) + &(page * 1000).to_string())
        .call()
        .expect("request falhou rede");

    if !response.status().is_success() {
        panic!("request falhou status");
    }

    response
        .body_mut()
        .read_json::<Response>()
        .expect("request falhou corpo")
}

#[derive(Debug, Deserialize)]
pub struct Response {
    took: u32,
    hits: ResponseHits,
}

#[derive(Debug, Deserialize)]
pub struct ResponseHits {
    hits: Vec<ResponseHit>,
}

#[derive(Debug, Deserialize)]
pub struct ResponseHit {
    #[serde(rename = "_score")]
    pub score: f64,
    #[serde(rename = "_source")]
    pub source: ResponseHitSource,
    pub fields: Option<ResponseHitFields>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseHitSource {
    pub id: u32,
    pub rent: u32,
    pub total_cost: u32,
    pub sale_price: u32,
    pub iptu_plus_condominium: u32,
    pub area: Option<u32>,
    pub image_list: Option<Vec<String>>,
    pub address: String,
    pub region_name: String,
    pub city: String,
    pub visit_status: ResponseHitSourceVisit,
    #[serde(rename = "type")]
    pub kind: Option<ResponseHitSourceKind>,
    pub for_rent: ResponseHitSourceCornoBool,
    pub for_sale: Option<ResponseHitSourceCornoBool>,
    pub is_primary_market: Option<bool>,
    pub bedrooms: Option<u8>,
    pub parking_spaces: Option<u8>,
    pub neighbourhood: String,
    pub bathrooms: Option<u8>,
    pub is_furnished: Option<ResponseHitSourceCornoBool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResponseHitSourceVisit {
    AcceptAlways,
    AcceptNew,
    Blocked,
}

#[derive(Debug, Deserialize)]
pub enum ResponseHitSourceKind {
    StudioOuKitchenette,
    CasaCondominio,
    Apartamento,
    Casa,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ResponseHitSourceCornoBool {
    Bool(bool),
    Corno(ResponseHitSourceFurnishedCorno),
}

impl ResponseHitSourceCornoBool {
    pub fn to_int(self) -> u8 {
        match self {
            ResponseHitSourceCornoBool::Bool(b) => match b {
                true => 1,
                false => 0,
            },
            ResponseHitSourceCornoBool::Corno(corno) => match corno {
                ResponseHitSourceFurnishedCorno::False => 0,
                ResponseHitSourceFurnishedCorno::True => 1,
            },
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResponseHitSourceFurnishedCorno {
    False,
    True,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseHitFields {
    pub listing_tags: Vec<String>,
}

// -19.9121089,-43.9149629
// -19.9560299,-43.9758439,
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
return=installations&\
page_size=1000&\
offset=";
