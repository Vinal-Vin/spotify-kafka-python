# CS424 Project

## Overview

This repository contains the source code and documentation for the CS424 project. The project aims to load Spotify data using their APIs and send the data to the Kafka server through the use of Producer. Jupyter Notebooks files are then utilized to consume data, store them in HDFS and
and analyze the data stored.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)

## Installation

To install the project, follow these steps:

1. Clone the repository:
   ```sh
   git clone https://github.com/yourusername/cs424_project.git
   ```
2. Navigate to the project directory:
   ```sh
   cd /path/to/your/project_folder
   ```
3. Install the required dependencies:
   ```sh
   pip install package_name
   ```

## Usage

To use the project, follow these steps:

1. Run the first main Python script:
   ```sh
   python artists.py
   ```
2. Run the second main Python script:
   ```sh
   python SpotifyProducer.py
   ```
