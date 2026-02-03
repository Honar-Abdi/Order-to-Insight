# Order-to-Insight (tiivistelmä)

Tämä projekti on esimerkki päästä päähän -dataputkesta, jossa raaka tapahtuma- ja tilaustieto jalostetaan analyysikelpoisiksi faktoiksi ja liiketoimintainsighteiksi.

Projektin painopiste on data engineeringin ja data-analytiikan perusasioissa: datan laatu, mallinnus, SQL-pohjainen analyysi sekä tulosten oikea tulkinta.

Kyseessä ei ole koneoppimisprojekti, vaan data-analytiikan ja analytiikka-insinöörityön perusteisiin keskittyvä kokonaisuus.

---

## Projektin tausta ja ongelma

Monissa organisaatioissa liiketoimintapäätökset perustuvat dataan, joka syntyy useissa eri järjestelmissä.  
Tilaukset, maksut ja tapahtumat kerätään usein erikseen, ja data sisältää tyypillisesti puutteita, ristiriitaisuuksia ja aikaleimaongelmia.

Tämä projekti simuloi tilannetta, jossa sisäinen datatiimi vastaa raakadatan muuntamisesta luotettavaksi analytiikkadataksi.

Keskeinen kysymys on:

**Miten raakamuotoinen tapahtuma- ja transaktiodata voidaan muuttaa luotettavaksi analyysiaineistoksi, ja miten mallinnusvalinnat vaikuttavat tehtäviin liiketoimintajohtopäätöksiin?**

---

## Projektin tavoitteet

Projektin tavoitteena on:

- havainnollistaa koko datan elinkaari ingestoinnista analyysiin
- näyttää käytännön data engineering -osaamista (validointi, transformaatio, mallinnus)
- vastata realistisiin liiketoimintakysymyksiin SQL:n avulla
- tuoda datan laatuongelmat näkyväksi analyysitasolla
- dokumentoida oletukset, rajoitteet ja riskit avoimesti

Projekti toimii oppimis- ja portfoliohankkeena data-analytiikan ja analytics engineering -rooleihin.

---

## Data lyhyesti

Projektissa käytetään kahta datatyyppiä:

- **Tapahtumadata** (event data): esim. tilauksen luonti, maksun vahvistus, toimitus, peruutus
- **Transaktiodata**: tilaukset, summat, tilausten tila ja asiakastunnisteet

Data on synteettisesti generoitu, mutta rakenteeltaan realistista.  
Mukana on tarkoituksella tyypillisiä ongelmia, kuten:
- puuttuvia tapahtumia
- ristiriitaisia tiloja
- duplikaatteja
- puuttuvia asiakastunnisteita

---

## Dataputken rakenne

Dataputki on jaettu selkeisiin vaiheisiin:

1. **Ingestointi**  
   Raakadata generoidaan Pythonilla ja tallennetaan muuttamattomana.

2. **Datan validointi**  
   Ajetaan datalaatusääntöjä, joilla tunnistetaan virheelliset ja epäilyttävät rivit.  
   Osa laatuongelmista viedään mallinnettuun dataan eksplisiittisinä lippuina.

3. **Transformaatio ja mallinnus**  
   Data muunnetaan SQL:llä analyysikelpoisiksi faktatauluiksi.

4. **Analyysi**  
   SQL-kyselyillä tuotetaan liiketoimintamittareita ja laadullisia havaintoja.

5. **Tulkinta**  
   Tulokset tulkitaan huomioiden datan rajoitteet ja mahdolliset virhelähteet.

DuckDB:tä käytetään paikallisena analytiikkatietokantana, jotta projekti pysyy toistettavana ja SQL-keskeisenä.

---

## Analyysin yhteenveto (nykyinen ajo)

Nykyinen esimerkkiajo on tehty **20 000 tilauksella**.

- Toteutuneet tilaukset: **17 848**
- Perutut tilaukset: **1 600**
- Palautetut tilaukset: **552**
- Toteutunut liikevaihto: **2 638 001**
- Keskimääräinen tilausarvo: **147,8**

Mallinnetussa datassa havaitaan myös laatuongelmia:
- **477** toteutunutta tilausta ilman maksutapahtumaa
- **31** peruttua tilausta, joilla on silti toimitustapahtuma

Nämä havainnot korostavat, miksi tapahtuma- ja transaktiodataa ei voi olettaa täysin yhdenmukaiseksi.

---

## Keskeiset opit

Projektin aikana keskeisiksi opeiksi nousivat:

- Datan laatu ja mallinnus vaikuttavat analyysin luotettavuuteen enemmän kuin monimutkaiset algoritmit.
- Yksinkertaiset aggregaatiot voivat johtaa vääriin johtopäätöksiin ilman kontekstia.
- Selkeä erottelu raakadatan, mallinnetun datan ja analyysin välillä parantaa ylläpidettävyyttä.
- Oletusten ja rajoitteiden dokumentointi on olennainen osa vastuullista data-analytiikkaa.

---

## Tuotantoympäristöä koskevat huomiot

Tuotannossa dataputkea laajennettaisiin mm.:

- automatisoidulla ajoituksella ja valvonnalla
- jatkuvalla datalaadun seurannalla
- testauksella ja versionhallinnalla
- käyttöoikeuksien hallinnalla

Analyysilogiikka itsessään olisi kuitenkin pitkälti sama.

---

## Riskit ja tulkintarajoitteet

- Puutteellinen tapahtumadata voi vääristää mittareita.
- Aikaleimaongelmat vaikuttavat aikaperusteisiin analyyseihin.
- Aggregaatiot voivat peittää alleen yksittäisiä ongelmatapauksia.

Tuloksia tulee aina tarkastella nämä rajoitteet huomioiden.
