# Order-to-Insight (tiivistelmä)

Tämä projekti on esimerkki päästä päähän -dataputkesta, jossa raaka tapahtuma- ja tilaustieto
jalostetaan analyysikelpoisiksi faktoiksi ja liiketoimintaa tukeviksi havainnoiksi.

Projektin painopiste on data engineeringin ja data-analytiikan perusasioissa: datan laatu,
mallinnus, SQL-pohjainen analyysi sekä tulosten oikea ja vastuullinen tulkinta.

Kyseessä ei ole koneoppimisprojekti, vaan kokonaisuus, joka keskittyy analytiikan ja
analytics engineering -työn ytimeen.

---

## Projektin tausta ja ongelma

Monissa organisaatioissa liiketoimintapäätökset perustuvat dataan, joka syntyy useissa eri
järjestelmissä. Tilaukset, maksut ja tapahtumat kerätään usein erillään, ja data sisältää
tyypillisesti puutteita, ristiriitaisuuksia ja aikaleimoihin liittyviä ongelmia.

Tämä projekti simuloi tilannetta, jossa sisäinen datatiimi vastaa raakadatan muuntamisesta
luotettavaksi analytiikkadataksi.

Keskeinen kysymys on:

**Miten raakamuotoinen tapahtuma- ja transaktiodata voidaan mallintaa luotettavaksi
analyysiaineistoksi, ja miten mallinnusvalinnat vaikuttavat tehtäviin
liiketoimintajohtopäätöksiin?**

---

## Projektin tavoitteet

Projektin tavoitteena on:

- havainnollistaa datan koko elinkaari ingestoinnista analyysiin
- osoittaa käytännön data engineering -osaamista (validointi, transformaatio, mallinnus)
- vastata realistisiin liiketoimintakysymyksiin SQL:n avulla
- tuoda datan laatuongelmat näkyväksi analyysitasolla
- dokumentoida oletukset, rajoitteet ja riskit avoimesti

Projekti toimii oppimis- ja portfoliohankkeena data-analytiikan ja analytics engineering
-rooleihin.

---

## Data lyhyesti

Projektissa käytetään kahta toisiaan täydentävää datatyyppiä:

- **Tapahtumadata (event data)**: esimerkiksi tilauksen luonti, maksun vahvistus,
  toimitus ja peruutus
- **Transaktiodata**: tilaukset, rahamäärät, tilausten tila ja asiakastunnisteet

Data on synteettisesti generoitu, mutta rakenteeltaan ja jakaumiltaan realistista.
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
   Datalle ajetaan laatusääntöjä, joilla tunnistetaan virheelliset ja epäilyttävät rivit.  
   Osa laatuongelmista viedään mallinnettuun dataan eksplisiittisinä indikaattoreina.

3. **Transformaatio ja mallinnus**  
   Data muunnetaan SQL:n avulla analyysikelpoisiksi faktatauluiksi.

4. **Analyysi**  
   SQL-kyselyillä tuotetaan liiketoimintamittareita ja laadullisia havaintoja.

5. **Tulkinta**  
   Tulokset tulkitaan huomioiden datan rajoitteet ja mahdolliset virhelähteet.

DuckDB:tä käytetään paikallisena analytiikkatietokantana, jotta projekti pysyy
toistettavana, kevyenä ja SQL-keskeisenä.

---

## Analyysin yhteenveto (nykyinen ajo)

Nykyinen esimerkkiajo on tehty **20 000 tilauksella**.

- Toteutuneet tilaukset: **17 848**
- Perutut tilaukset: **1 600**
- Palautetut tilaukset: **552**
- Toteutunut liikevaihto: **2 638 001**
- Keskimääräinen tilausarvo: **147,8**

Mallinnetussa datassa havaitaan myös eksplisiittisiä laatuongelmia:

- **477** toteutunutta tilausta ilman maksutapahtumaa
- **31** peruttua tilausta, joilla on silti toimitustapahtuma

Nämä havainnot korostavat, miksi tapahtuma- ja transaktiodataa ei voida olettaa
täysin yhdenmukaiseksi tai täydelliseksi.

---

## Visualisoinnit ja analyysin validointi

SQL-pohjaisen analyysin lisäksi projektiin on lisätty rajattu joukko
visualisointeja, joiden tarkoituksena on analyysin validointi ja tulkinnan tukeminen,
ei raportointi tai dashboardointi.

### Toteutunut liikevaihto ajan yli

Päivittäinen aikasarja toteutuneesta liikevaihdosta toimii tarkistuksena
aggregointilogiikalle, ajalliselle jatkuvuudelle sekä datan reunaefekteille.

**Tulkinta:**  
Visualisointi osoittaa liikevaihdon pysyvän rakenteellisesti vakaana ja tuo
selkeästi esiin osittaiset päivät datan alku- ja loppupäässä.

---

### Tapahtumakattavuus tilauksissa

Visualisointi näyttää, kuinka suuri osuus tilauksista sisältää kunkin
elinkaaritapahtuman.

**Tulkinta:**  
Kaikilla toteutuneilla tilauksilla ei ole vastaavaa maksun tai toimituksen
tapahtumaa. Tämä tekee näkyväksi riskin, joka syntyy, jos analyysi perustuu
yksinomaan tapahtumadataan.

---

### Puuttuvat elinkaaritapahtumat toteutuneissa tilauksissa (jatkotarkastelu)

Tässä näkymässä tarkastellaan vain toteutuneita tilauksia ja sitä,
kuinka monelta niistä puuttuu maksun tai toimituksen tapahtuma.

**Tulkinta:**  
Vaikka tilaus on liiketoiminnan näkökulmasta toteutunut, tapahtumadata ei ole
välttämättä täydellistä. Tämä tukee mallinnusratkaisua, jossa tilauksen tila
toimii ensisijaisena liiketoimintasignaalina ja tapahtumat tukevana tietona.

---

## Keskeiset opit

Projektin aikana keskeisiksi opeiksi nousivat:

- Datan laatu ja mallinnus vaikuttavat analyysin luotettavuuteen enemmän kuin
  monimutkaiset algoritmit.
- Yksinkertaiset aggregaatiot voivat johtaa virheellisiin johtopäätöksiin ilman
  kontekstia ja validointia.
- Selkeä erottelu raakadatan, mallinnetun datan ja analyysin välillä parantaa
  ylläpidettävyyttä ja luotettavuutta.
- Oletusten ja rajoitteiden dokumentointi on olennainen osa vastuullista
  data-analytiikkaa.

---

## Tuotantoympäristöä koskevat huomiot

Tuotantoympäristössä dataputkea laajennettaisiin muun muassa:

- automatisoidulla ajoituksella ja valvonnalla
- jatkuvalla datalaadun seurannalla
- testauksella ja versionhallinnalla
- käyttöoikeuksien hallinnalla

Analyysilogiikka ja mallinnusperiaatteet säilyisivät kuitenkin pääosin samoina.

---

## Riskit ja tulkintarajoitteet

- Puutteellinen tapahtumadata voi vääristää konversio- ja aikamittareita.
- Aikaleimaongelmat vaikuttavat aikaperusteisiin analyyseihin.
- Aggregaatiot voivat peittää alleen yksittäisiä poikkeustapauksia.

Tuloksia tulee aina tarkastella nämä rajoitteet huomioiden.
