<a id="future-data"></a>

# High Priority Target Datasets

This page lists datasets that we’ve identified for future integration into PUDL,
including some which we’ve started work on but are not yet to the point of being
available in the main PUDL database which we distribute.

#### IMPORTANT
Looking for a specific dataset?

If you **need data that’s not in PUDL**, [open an issue](https://github.com/catalyst-cooperative/pudl/issues/new?assignees=&labels=new-data&projects=&template=new_dataset.md&title=)
to tell us more about it!

If you’ve **already spent a bunch of time wrangling a dataset**, we welcome
“knowledge contributions” in our [pudl-knowledge](https://github.com/catalyst-cooperative/pudl-knowledge) repository!

If you’re **looking to help us integrate a specific dataset into PUDL**, find us at
[office hours](https://calend.ly/catalyst-cooperative/pudl-office-hours) and we
can talk through next steps.

There’s a huge variety and quantity of data about the US electric utility system
available to the public. The data we have integrated is just the beginning! Other data
we’ve heard demand for are listed below. If you’re interested in using one of them and
would like to add it to PUDL check out [our contribution guidelines](../CONTRIBUTING.md). If there are other datasets you think we should be looking at
integration, don’t hesitate to [open an issue on Github](https://github.com/catalyst-cooperative/pudl/issues) requesting the data and
explaining why it would be useful.

<a id="data-tds"></a>

## Transmission and Distribution Systems

In order to run electricity system operations models and cost optimizations, you need
some kind of model of the interconnections between generation and loads. There doesn’t
appear to be a generally accepted, publicly available set of these network descriptions
(yet!).

<a id="data-eiawater"></a>

## EIA Thermoelectric Water Usage

[EIA Water](https://www.eia.gov/electricity/data/water/) records water use by thermal
generating stations in the US.

<a id="data-msha"></a>

## MSHA Mines and Production

The [MSHA Mines & Production](https://arlweb.msha.gov/OpenGovernmentData/OGIMSHA.asp)
dataset describes coal production by mine and operating company along with statistics
about labor productivity and safety. This is a smaller dataset (100s of MB) available as
relatively clean and well structured CSV files.

<a id="data-ces"></a>

## Machine Readable Clean Energy Standards

[Renewable Portfolio Standards (RPS)](https://www.ncsl.org/research/energy/renewable-portfolio-standards.aspx)
and Clean Energy Standards (CES) have emerged as one of the primary policy tools to
decarbonize the US electricity supply. Researchers who model future electricity systems
need to include these binding regulations as constraints on their models to ensure that
the systems they explore are legally compliant. Unfortunately for modelers, RPS and CES
regulations vary from state to state. Sometimes there are carve outs for different types
of generation, and sometimes there are different requirements for different types of
utilities or distributed resources. Our goal is to compile a programmatically usable
database of RPS/CES policies in the US for quick and easy reference by modelers.
