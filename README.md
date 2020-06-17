# 10x-MeL
10xMLaaS project: MeL is a machine learning and natural language processing tool for analyzing open text data.

## Setup and Usage

### How to install

1. Install **Docker Desktop**
   1. Go to https://www.docker.com/products/docker-desktop
   2. Install the **Docker Desktop** for your platform
   3. Make sure Docker is running
2. Install **git** 
   1. Go to https://git-scm.com/download/
   2. Install the **git** for your platform
3. Create a folder on your machine for the **MeL** tool
   1. Choose a location for the **MeL** tool on your machine, for example, in your **Documents** folder.
   2. Open a **Terminal** (Mac/Linux) or **Command Prompt** (Windows) and navigate to the folder you chose in the previous step:
      -  `cd path/to/that/folder`
      - for example, on a Mac: `cd ~/Documents`
   3. Create a new folder for the **MeL** tool, for example `mel`:
      - `mkdir mel`
   4. Move to the newly created folder:
      -  `cd mel` 
4. Obtain the **Mel** tool from **GitHub**
   1. Type the following to copy the **MeL** tool to your machine:
      - `git clone https://github.com/18F/10x-MeL.git`
   2. Navigate into the tool by typing:
      - `cd 10x-MeL`
5. Connect the **MeL** tool to Docker by typing:
   - `docker-compose up --build -d`

### Using the tool

1. Open **Docker Desktop**
2. Observe the listing for `10x-mel` and the presence of a **play button**
3. Click the **play button**
4. Once the status is `RUNNING` or the tool icon turns **green**, go to
   - http://localhost:5000/
   - This page can also be opened but double-clicking `start.webloc` in the `MeL` folder 

## Tools

### AutoCat

#### Purpose

To discover latent categories in the text and assign entries to those categories.

#### Approach

There are several components to automatic categorization:

##### Category Discovery

		1. The text in each entry is parsed and noun phrases are extracted
  		2. Count occurrences of words and phrases contained in the noun phrases
       - Counts are boosted according to the the recency of the entries in which they appear
  		3. Use the most commonly occurring words and phrases as the initial category headings
  		4. Perform an initial pass over the categories, merging those categories that share common terms
  		5. Create language models for each category
  		6. Perform another pass over the categories, this time merging categories whose language models are similar, using entropy as a metric

##### Entry Prediction

  		1. Entries are compared to the terms in the most popular categories, according to a power-law "rich get richer" approach. When terms intersect, the entry is linked to one or more category/subcategory pairs.
  		2. If an entry fails to match any categories, a language model is created for the entry and it is compared with the language model of each category to determine best fit.

### Problem Report

#### Purpose

To detect user reports of problem they encounter with usa.gov and other government websites

#### Approach

##### Data

Two categories of data are used to detect problems:

1. The text of users' survey responses
2. The ratings provided by users, often on a scale from 1 to 5
   - These ratings are normalized from a scale of 1 to 5 to a scale from -2 to +2, where complaints are negative.

##### Evaluation

The task is distilled into two subtasks:

###### Determining whether the entry provides a negative context

- example: a complaint

Linguistic analysis is combined with the users' ratings to determine the sentiment of the entry and the likelihood that the user is providing context about a problem that was experienced

###### Determining relevance

- example: a government website provides out-of-date information

Surface pattern matching is used here to determine whether the content of the entry is relevant. This matching is list based, so it can be easily adapted for other use cases.

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for additional information.

## Public domain

This project is in the worldwide [public domain](LICENSE.md). As stated in [CONTRIBUTING](CONTRIBUTING.md):

> This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).
>
> All contributions to this project will be released under the CC0 dedication. By submitting a pull request, you are agreeing to comply with this waiver of copyright interest.
