# Order-to-Insight (tiivistelmä)

## TL;DR

Tämä projekti rakentaa rakenteellisen analytiikkaputken Pythonin, SQL:n ja DuckDB:n avulla.

Raaka tilaus- ja tapahtumadata validoidaan, mallinnetaan ja muunnetaan
luotettaviksi faktatauluiksi, joita voidaan käyttää liiketoimintaraportointiin.

Painopiste on:

- SQL-lähtöisessä mallinnuksessa  
- eksplisiittisessä datan laadun validoinnissa  
- selkeissä ja testattavissa metriikkamääritelmissä  
- siinä, miten mallinnusvalinnat vaikuttavat raportoituihin lukuihin  

Liikevaihto lasketaan tarkoituksella usealla tavalla, jotta voidaan osoittaa,
miten datan täydellisyys ja metriikan logiikka vaikuttavat liiketoimintatuloksiin.

Projekti soveltuu erityisesti analytics engineering- ja data engineering -rooleihin,
joissa korostuvat oikeellisuus, mallinnuksen selkeys ja liiketoimintalogiikan ymmärrys.

---

## 1. Ongelma

Monissa järjestelmissä liiketoimintamittarit perustuvat useista lähteistä
peräisin olevaan dataan:

- Transaktiodata (tilaukset, summat, tilat)  
- Tapahtumadata (luonti, maksu, toimitus, peruutus)

Lähteet ovat usein:

- puutteellisia  
- keskenään ristiriitaisia  
- aikaleimoiltaan epäyhtenäisiä  

Jos mallinnuslogiikkaa ei määritellä selkeästi, sama liikevaihto voidaan
raportoida eri tavoin riippuen käytetystä määritelmästä.

Projektin keskeinen kysymys on:

> Miten raakamuotoinen tilaus- ja tapahtumadata voidaan mallintaa
> luotettaviksi analytiikkatauluiksi, ja miten mallinnusvalinnat
> vaikuttavat liiketoimintamittareihin?

---

## 2. Mitä projektissa on rakennettu

Projekti toteuttaa kerroksellisen dataputken viidessä vaiheessa:

1. **Ingestointi**  
   Synteettinen mutta rakenteellisesti realistinen data generoidaan.

2. **Datan validointi**  
   Laatusäännöillä tunnistetaan puuttuvat ja ristiriitaiset rivit.

3. **SQL-mallinnus**  
   Tapahtumat ja tilaukset yhdistetään faktatauluiksi.

4. **Analyysikerros**  
   SQL-kyselyillä tuotetaan liiketoimintamittareita.

5. **Visualisointi**  
   Mallinnuksen oikeellisuutta validoidaan rajatuilla kuvaajilla.

Kaikki suoritetaan paikallisesti DuckDB:n avulla.

---

## 3. Datamalli

Mallinnettu analytiikkakerros sisältää muun muassa:

- `fct_orders`
- `fct_daily_revenue`

Datalaatuongelmat tuodaan näkyviin eksplisiittisinä indikaattoreina, kuten:

- toteutuneet tilaukset ilman maksutapahtumaa  
- perutut tilaukset, joilla on toimitustapahtuma  

Näitä ongelmia ei piiloteta, vaan ne mitataan ja dokumentoidaan.

---

## 4. Keskeiset analyysihavainnot

### 4.1 Tapahtumakattavuus

Kaikilla toteutuneilla tilauksilla ei ole vastaavaa maksun
tai toimituksen tapahtumaa.

Tämä tarkoittaa, että pelkkään tapahtumadataan perustuva analyysi
voi aliarvioida sekä tilauksia että liikevaihtoa.

---

### 4.2 Liikevaihdon määritelmä

Liikevaihto voidaan laskea kahdella tavalla:

1. **Tilaan perustuva liikevaihto**  
   Kaikki toteutuneet tilaukset.

2. **Tapahtumaan perustuva liikevaihto**  
   Vain toteutuneet tilaukset, joilla on maksutapahtuma.

Koska osa toteutuneista tilauksista puuttuu maksutapahtuma,
nämä kaksi lukua eivät ole samat.

Keskeinen havainto ei ole pelkästään erotuksen suuruus,
vaan se, että metriikan määritelmä vaikuttaa raportoituihin tuloksiin.

---

### 4.3 Datan laatu → metriikkamääritelmä → päätöksenteko

Projekti havainnollistaa seuraavan ketjun:

Datan laatu  
→ vaikuttaa mittarin rakentamiseen  
→ vaikuttaa raportoituihin lukuihin  
→ vaikuttaa liiketoimintapäätöksiin  

Ilman selkeää mallinnusta ja validointia mittarit voivat
näyttää oikeilta, vaikka niiden taustalla on epäjohdonmukaisuutta.

---

## 5. Visualisointi ja validointi

Visualisointien tarkoitus on analyysin validointi, ei dashboardointi.

### Päivittäinen toteutunut liikevaihto

Vahvistaa aggregointilogiikan ja ajallisen jatkuvuuden.

### Tapahtumakattavuus

Näyttää, kuinka suuri osuus tilauksista sisältää
elinkaaritapahtumat.

### Liikevaihdon määritelmien ero

Havainnollistaa, että raportoitava liikevaihto
riippuu käytetystä määritelmästä.

---

## 6. Mitä projekti osoittaa

Projekti osoittaa:

- kyvyn suunnitella kerroksellinen dataputki  
- SQL-pohjaisen mallinnusosaamisen  
- datalaadun systemaattisen käsittelyn  
- metriikkamääritelmien vaikutuksen liiketoimintaan  
- analyysin dokumentoinnin ja tulkinnan  

---

## 7. Tuotantoympäristö

Tuotantoympäristössä kokonaisuutta laajennettaisiin:

- automatisoidulla ajoituksella  
- jatkuvalla datalaadun seurannalla  
- testauksella ja versionhallinnalla  
- käyttöoikeuksien hallinnalla  

Mallinnusperiaatteet säilyisivät samoina.

---

## 8. Johtopäätös

Liikevaihto ei ole yksiselitteinen luku.

Se riippuu siitä, miten se määritellään ja mihin dataan se perustuu.

Tämä projekti osoittaa, että:

- datan täydellisyys on mitattava  
- metriikkamääritelmät on dokumentoitava  
- analyysin luotettavuus rakentuu mallinnuksen selkeydestä  

Ilman tätä perustaa raportointi voi olla epäjohdonmukaista,
vaikka data näyttäisi ensi silmäyksellä kunnolliselta.
