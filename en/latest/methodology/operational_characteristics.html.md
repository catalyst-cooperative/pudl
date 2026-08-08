# Generator Operational Characteristics

## Overview

Production cost models, capacity expansion models, and other power system planning
tools generally need more than a generator’s nameplate capacity to represent how it
actually behaves on the grid. They need estimates of things like how low a unit can
run without shutting down, how long it tends to stay on or off once it starts a run,
how quickly it can change output, and how its fuel efficiency changes across its
operating range. These “operational characteristics” (sometimes called unit
commitment parameters) aren’t reported directly by EIA or FERC, so PUDL estimates them
empirically from [EPA Hourly Continuous Emission Monitoring System (CEMS)](../data_sources/epacems.md) hourly generation and fuel data, in the
[out_epacems_\_yearly_operational_characteristics](../data_dictionaries/pudl_db.md#out-epacems-yearly-operational-characteristics) table.

This methodology and its original implementation were generously contributed to PUDL by
[Sylvan Energy](https://sylvan.energy), with support for integration from [GridLab](https://gridlab.org),. We’ve adapted Sylvan Energy’s analysis to run automatically
across every EPA CEMS reporting unit in the country as part of the regular PUDL
pipeline. The table is still marked experimental while this integration continues. We
describe the specific choices behind these metrics below so you can judge whether they
suit your use case. [Feedback Welcome](#feedback-welcome)!

This analysis processes several years of hourly readings for every EPA CEMS unit in the
country using [polars](https://pola.rs), a vectorized DataFrame library, reading the
input data from [Apache Parquet](https://parquet.apache.org/docs/) files. The full
calculation completes in 1-3 minutes and peaks at around 16 GB of memory.

## Scope: EPA CEMS Units, Gross Generation

These estimates describe individual EPA CEMS “emissions units” (aka smokestacks),
identified by `plant_id_epa` and `emissions_unit_id_epa`, **not** EIA generators. A
CEMS unit doesn’t always correspond one-to-one with an EIA generator; see
[core_epa_\_assn_eia_epacamd](../data_dictionaries/pudl_db.md#core-epa-assn-eia-epacamd) if you need to connect these characteristics to EIA
generator-level records.

Only fossil-fuel combustion units over 25 MW are required to report to EPA CEMS, so
this table only covers that population – it doesn’t include smaller fossil units, and
it doesn’t include non-combustion generation (e.g. wind, solar, hydro, nuclear) at all.

These characteristics are currently derived from CEMS’s directly monitored **gross**
generation and fuel heat input, since that’s what CEMS reports hourly at the smokestack.
Sylvan Energy has also contributed a method for converting gross to net generation,
which is under active development and not yet integrated into PUDL. Once that lands,
these tables will also offer net-generation-based versions of these characteristics,
which will be more directly comparable to the net-generation accounting used elsewhere
in PUDL.

## A Rolling Window

This table recomputes a single snapshot of each unit’s characteristics from a rolling
window of the most recent EPA CEMS data available – in production, the three most
recently completed calendar years. The `report_year` column records the vintage of
that snapshot (the most recent year included in the window). We plan to extend this
methodology to cover all available years, rather than just the most recent window, in
the near future.

<a id="load-factor-bins"></a>

## Load Factor Bins

Several of these characteristics are defined relative to a unit’s **load factor**:
its gross load in a given hour, divided by the highest gross load it reached anywhere
in the analysis window. Each unit’s own observed load factor range is divided into ten
equal-width bins, and behavior is characterized within each bin (e.g. what fraction of
hours fall in the lowest bin, how heat rate varies bin to bin).

These bins are scaled to each unit’s own observed operating range, rather than fixed,
absolute bins shared across all units (e.g. 0-10% of some standard capacity, 10-20%, and
so on). A large baseload steam plant and a small peaking combustion turbine typically
operate over very different absolute ranges, and scaling the bins to each unit describes
both in comparable, unit-specific terms. (Note that these load-factor bins are
completely distinct from the ramp-rate bins described below in [Ramp Rates](#ramp-rates)).

## Minimum Stable Load

A unit’s `min_stable_load_factor` is defined empirically: it’s the lowest load-factor
bin in which the unit is observed sustaining operation for at least a minimum number of
consecutive hours (8 hours by default, though this threshold is configurable). This
reflects flexibility the unit has *actually demonstrated* during the window, rather than
a nameplate turndown rating, which may be more or less conservative than what’s observed
in practice.

Because this is based on directly observed behavior, a unit that has simply never had
occasion to run for very long at low output will show a higher (less flexible) minimum
stable load than it might technically be able to sustain.

## Minimum Up and Down Time

Once a unit’s minimum stable load is known, every hour at or above that level counts as
“up,” and every hour at zero load counts as “down.” Minimum up time and minimum down
time are each the *shortest* uninterrupted run of “up” or “down” hours observed
anywhere in the window, rather than a typical or median run length. These statistics
are most often used as hard constraints in production cost and unit commitment models
(e.g. a unit *cannot* turn off after less than its minimum up time), which is the main
reason for reporting the shortest observed run rather than a typical one.

Because it’s a minimum taken over many observed runs, a longer window that captures more
up/down cycles is more likely to reveal a short, unusual run – so units may look like
they’re becoming increasingly flexible as more historical data is included, independent
of any real change in how the unit is operated.

## Heat Rates

Heat rate – fuel heat input divided by gross generation (MMBtu/MWh) – is a standard
proxy for thermal efficiency; lower is more efficient. Rather than a unit’s full,
continuous heat-rate curve across its entire operating range, two representative points
are reported: the median heat rate observed while operating in the unit’s highest
load-factor bin (`heat_rate_at_max_load_factor_mmbtu_per_mwh`), and the median heat
rate observed while operating in its minimum stable bin
(`heat_rate_at_min_stable_load_factor_mmbtu_per_mwh`). Comparing the two gives a sense
of how much efficiency degrades at low output relative to full output, without claiming
to characterize everything in between. The median, rather than the mean, is used within
each bin to reduce the influence of unusual hours (e.g. startup transients) that don’t
reflect a unit’s steady operation at that load level.

<a id="ramp-rates"></a>

## Ramp Rates

Ramp rates describe how quickly a unit can change output, expressed here **as a fraction
of its maximum observed capacity per minute**, which makes them comparable across units
of very different sizes. They’re calculated only from hours when the unit was operating
above its minimum stable level, which excludes the more extreme ramps associated with
startup and shutdown events.

Rather than the single fastest ramp ever observed (likely an unusual one-off event) or
an average ramp rate (which would be dominated by the much more common, comparatively
slow, steady-state adjustments), the **median of the fastest 5% of observed upward
ramps** is reported, and separately the **median of the fastest 5% of observed downward
ramps**. This is meant to represent a ramp rate the unit can practically and repeatedly
achieve, rather than either an extreme outlier or its typical, more leisurely pace of
adjustment. Units that never operated flexibly enough during the window to produce a
reasonable number of ramping events won’t have a ramp rate estimate at all, rather than
one based on too few observations to be meaningful.

To find the fastest 5%, observed ramp rates are sorted and split into 20 equal-count
(quantile) groups, and only the two extreme groups – fastest upward and fastest
downward – are used. (Note that this is entirely distinct from the
[Load Factor Bins](#load-factor-bins) above mentioned above).

<a id="feedback-welcome"></a>

## Feedback Welcome

This is a newer, more experimental corner of PUDL, and for now we’ve focused on
faithfully translating and productionizing Sylvan Energy’s original analysis rather than
trying to evaluate any of the specific decisions it made. We don’t have as much domain
expertise in power system operations and modeling as the people who actually use data
like this every day, so we’d like to draw on the broader community of energy system data
users to help evaluate and refine these choices – the rolling window length, the
minimum-stable-level threshold, the ramp rate percentile, gross vs. net generation, or
anything else described above. If any of it doesn’t match what you need, or you have
ideas for how it could be improved, please get in touch!
