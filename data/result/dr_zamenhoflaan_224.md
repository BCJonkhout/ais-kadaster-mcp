# Location Analysis: Dr. Zamenhoflaan 224, Enschede

This document contains property and environmental data for the location Dr. Zamenhoflaan 224, 7522 KW, Enschede, retrieved from the Kadaster Knowledge Graph (KKG).

## Address Details
- **Street:** Dr. Zamenhoflaan
- **House Number:** 224
- **Postal Code:** 7522 KW
- **City:** Enschede
- **Neighborhood:** Bolhaar (BU01530401)

## Building Information (BAG)
- **Object Type:** Gebouw (Building)
- **Primary Function:** Woonfunctie (Residential)
- **Year of Construction:** 1971
- **Total Floor Area:** 202 m²
- **Status:** Pand in gebruik / Bestaand

## Cadastral Information (BRK)
- **Parcel Number:** 5566
- **Section:** B
- **Total Plot Size:** 631 m²
- **Metric Area:** 701 m²

## Geographic Context (BGT)
- **Coordinates (WGS84):** Derived from geometry polygon.
- **Site Layout:** The parcel contains the main residential building and at least one secondary structure (overig gebouwtype), typical for detached or semi-detached housing in this area.

## SPARQL Query Used
```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX imxgeo: <http://modellen.geostandaarden.nl/def/imx-geo#>
PREFIX ext: <https://modellen.kkg.kadaster.nl/def/imxgeo-ext#>

SELECT 
  ?straat ?huisnummer ?postcode ?plaats 
  ?bouwjaar ?vloeroppervlakte ?gebruiksdoel
  ?perceelnummer ?perceelOppervlakte
  ?buurtNaam
WHERE {
  ?adres a imxgeo:Adres ;
    imxgeo:postcode "7522KW" ;
    imxgeo:huisnummer 224 ;
    imxgeo:straatnaam ?straat ;
    imxgeo:plaatsnaam ?plaats ;
    imxgeo:postcode ?postcode ;
    ext:vloerOppervlakte ?vloeroppervlakte .

  ?gebouw imxgeo:heeftAlsAdres ?adres ;
    a imxgeo:Gebouw ;
    imxgeo:bouwjaar ?bouwjaar ;
    imxgeo:gebruiksdoel ?gebruiksdoel .

  ?gebouw imxgeo:bevindtZichOpPerceel ?perceel .
  
  ?perceel a imxgeo:Perceel ;
    ext:perceelnummer ?perceelnummer ;
    geo:hasMetricArea ?perceelOppervlakte .

  ?perceel imxgeo:ligtInRegistratieveRuimte ?buurt .
  ?buurt a imxgeo:Buurt ;
    rdfs:label ?buurtNaam .
}
LIMIT 1
```

---
*Data retrieved on 2026-01-21 using the Kadaster Knowledge Graph Expert MCP.*
