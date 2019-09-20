
## Targeting Coal Plants for CES

CES Has asked for cost data from several sets of coal plants. Some selected by them, and some selected by Catalyst Cooperative:
#### CES selected the following 11 plants from the Western US:
1. Cooper
- George Neil
- Gerald Gentleman
- Holcomb
- Jeffrey
- Nebraska City
- Oklaunion
- Rodemacher
- San Juan
- Sibley
- Sooner

#### Catalyst selected 13 PJM plants
1. Amos
- Chesterfield
- East Bend
- Harrison
- HL Spurlock
- JM Stuart
- Killen
- La Cygne
- Mitchell
- Mt. Storm
- Rockport
- Virginia City
- Yorktown

#### CES Requested all PRPA, PSCo & PacifiCorp coal plants located in CO, UT, NM, MT, & WY:
1. Colstrip (PacifiCorp)
- Comanche (PSCo)
- Craig (PacifiCorp)
- Dave Johnston (PacifiCorp)
- Hayden (PSCo)
- Hunter (PacifiCorp)
- Huntington (PacifiCorp)
- Jim Bridger (PacifiCorp)
- Martin Drake (Bonus: Colorado Springs)
- Naughton (PacifiCorp)
- Pawnee (PSCo)
- Rawhide (PRPA)
- Wyodak (PacifiCorp)

The plants selected by Catalyst had the following attributes:
* Experienced fuel costs of $20/MWh or more in 2015 or 2016.
* Experienced capacity factors of < 60% in 2015 or 2016.
* Had a nameplate capacity of >= 100 MW.

## Notes on the selected plants:

### Amos:
  - **amos_1** has an unusually high heat rate in EIA 2009 data.

### Chesterfield:
  - Missing 2015-2016 EIA data for chesterfield 1 & 3 (NA values?)
  - Heat rate for Chesterfield 1 is way high comapred to FERC.
  - EIA capacity factor for unit 1 and 2 much lower than FERC.
  - Chesterfield is 4 units of different sizes:
    - 1: 112.5 MW
    - 2: 187.5 MW
    - 3: 359 MW
    - 4: 694 MW
  - The FERC numbers are averaged over the entire plant, while the
    EIA numbers are per unit. It's not surprising that especially
    Unit 1 is so far off, with lower CF and higher HR than the other
    units, since it's so small. These differences between the FERC &
    EIA numbers appear to be internally self-consistent. Summing up
    all the capacity and all of the generation across the 4 EIA units
    yields capacity factors that are within ~3% of the plant-wide
    capacity factor that we get from the FERC reporting.

### Colstrip:
  - The FERC data for **colstrip** has unusually low heat rate in 2004 and 2008. In both years this appears to be because the **colstrip_nwc** reporting of total_mmbtu is too low by several orders of magnitude, but it's not clear what kid of error is involved.
  - All **colstrip** units are absent from the EIA 923 data. Probably because of NA value contamination.

### Comanche:
 - In the FERC reporting, **comanche** fuel costs drop precipitously in 2016 -- i.e. by about *half*, which is hard to imagine. In EIA reporting, they do not. This seems suspicious.
 - Slight drop in FERC heat rates in 2016.
 - **comanche_3** only shows up in the EIA data after 2011, because that's when it came online? Thought it was more in the 2009-2010 timeframe.

### Craig:
 - Three generating units, owned by multiple parties.
 - Data is complete and highly consistent between FERC & EIA.

### Dave Johnston:
 - Four generating units, owned by PacifiCorp.
 - Data is clean, complete, and consistent between both EIA & FERC.

### East Bend:
  - Missing EIA data from 2013 (NA values?)

### George Neal:
  - Units 1 & 2:
    - missing EIA data for 2016.
    - missing FERC data prior to 2011.
    - CF goes to zero in 2016 for FERC.
    - heat rate spikes in 2016, as do fuel an OpEx costs per MWh
    - All of the above appear to be the result of these two units retiring early in 2016. Generation reported to FERC is miniscule, as are fuel costs.

### Gerald Gentleman:
  - Public Power, so no FERC.
  - EIA data is complete and looks reasonable.

### Harrison:
  - FERC net_generation_MWh appears to have been reported in the wrong units -- needs to be multiplied by 1000 (maybe they reported MWh rather than kWh...) which also messed up the heat rates (FIXED)
  - Missing all EIA data 2009-2014 (NA values?).
  - FERC capacity jumps from 421MW to 2050MW in 2013.
  - This is when FirstEnergy bought the plant from Allegheny Energy Supply
  - Allegheny Energy Supply was a merchant generator... not reporting to FERC?
  - Big dip in 2013 probably because it wasn't 2000MW for the entire year. Deal only closed in August of that year.

### Hayden
  - Missing data for 2014 in EIA (NA values?) for both **hayden_1** and **hayden_2**. Missing EIA data for 2009 for **hayden_1** as well.
  - Unreasonably low heat rate in FERC for 2004-2005.
  - **hayden_1** has capacity factor of slightly more than 1.0 in 2010 EIA data

### HL Spurlock:
  - Co-op plant, only EIA
  - No EIA data for 2013 or 2015 (unit 1).
  - No EIA data for 2015 (unit 2).

### Holcomb:
  - Public plant, only EIA data.
  - All EIA data present, appears reasonable.

### Hunter
  - Clean and consistent data where there's overlap between the FERC & EIA, but for some reason missing 2014-2016 EIA data (again, probably the NA contamination in the annual aggregation). True for all 3 generation units.

### Huntington
  - Clean and consistent data where there's overlap between the FERC & EIA, but for some reason missing 2014-2016 EIA data (again, probably the NA contamination in the annual aggregation). True for both generating units.

### Jeffrey:
  - Nothing particularly out of the ordinary. Data complete and relatively self consistent.

### Jim Bridger:
  - Consistent and complete data where it overlaps between FERC & EIA, for all 4 generating units.
  - total_mmbtu as reported to FERC in 2006 is a factor of ~700 too low. Not clear why that would be, but fuel costs & net generation are not different for that year... so this field is clearly incorrect.

### JM Stuart:
  - FERC 2015 fuel costs are off by a factor of 1000x (FIXED)

### Killen:
  - FERC 2015 fuel costs are off by a factor of 1000x (FIXED)
  - Missing 2014 EIA data.

### La Cygne 1:
  - KCPL FERC total mmBTU off by a factor of 500x (2x b/c 50% each plant, 100x b/c mmbtu?) but only for 2010-2011.

### La Cygne 2:
  - KCPL FERC total mmBTU off by a factor of 300x for 2010-2011. Not entirely clear why.

### Martin Drake 1-3:
  - Public power plant, so no FERC information.
  - **martin_drake_1** capacity factor drops to zero in 2016, and heat rate rises to 13mmBTU/MWh.
  - The other two generating units seem pretty consistent across all the EIA years.

### Mitchell 1 & 2:
  - EIA 2009 heat rate for Unit 1 is a bit low. For Unit 2 it's a bit high.
    Together, they seem to each other out.  Seems like there might be some
    kind of cross-unit reporting error, but it's not very significant.

### Mt. Storm:
  - Missing EIA data for 2015-2016 (NA values?)

### Naughton 1-3
  - Clean and consistent data across all years. Complete for both FERC & EIA.

### Nebraska City:
  - Public Power. No FERC data.
  - EIA data is complete and looks reasonable.

### Okalunion:
  - Data is complete, consistent between FERC & EIA, and looks reasonable.

### Pawnee:
  - FERC & EIA data are essentially identical. Data is complete and clean.

### Rockport:
  - Unit 1: Complete, consistent across FERC & EIA, and hella expensive (!)
  - Unit 2: Complete & hella expensive. EIA fuel costs for 2011 almost 2x the reported FERC fuel costs, not clear why...

### Rawhide:
  - Public power (PRPA) so no FERC data.
  - Data is clean, consistent, complete for EIA. Also, this is a cheap plant running at high capacity factor.

### Rodemacher:
  - FERC heat rates seem a bit too high in 2011.
  - FERC heat rates definitely too low in 2012 (9.0 mmBTU/MWh). But not insane.

### San Juan:
  - Both EIA & FERC Fuel costs are cut in half in 2016, for all 4 EIA units.
  - Not sure why that would be, but it's consistent across both data sources, even though heat rates and capacity factors remains similar across the entire set of years.

### Sibley:
  - Missing EIA for 2009, 2014-2016. Missing FERC for 2010-2011.
  - Only 2 overlapping years of data (2012-2013)
  - EIA data is missing because of NA values reported.
  - Not sure why the FERC data is being dropped (probably data quality)
  - Data that does exist appears relatively reasonable.

### Sooner:
  - Public power, no FERC data. EIA data is complete and reasonable.

### Virgina City:
  - EIA data completely missing (NA values?)
  - FERC heat rates ridiculously high for 2012. However, that was the first year of operations, and it started up in July, and it burns a variety of fuels including "Coal Gob" (from Gobco LLC!) so it may just be an artifact of commissioning.

### Wyodak:
  - NA values wiped out all the EIA data.
  - FERC heat rate is abnormally high for 2007.

### Yorktown:
  - Missing FERC data for 2012.
  - All FERC expenses become ridiculous in 2015 on a $/MWh basis.
  - YT3 is missing all EIA fuel cost data (NA values?)
  - EIA capacity factor is awfully close to 0 for YT3, but a bit higher than the FERC capacity factor (which is for all units) on YT1 & YT2, so overall net generation / capacity seems to be in the right universe.
  - FERC heat rate for 2004 is about a factor of 2 too low,
