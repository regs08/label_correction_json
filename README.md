# Label Correction Pipeline

A modular pipeline to download labels.json files from Azure Blob Storage, correct them using ground truth data, and upload the corrected files back to Azure.

## Project Structure

```
.
├── data/
│   ├── ground_truth/     # Place your ground truth CSV files here
│   └── temp/             # Temporary files during processing
├── src/
│   ├── pipeline/         # Prefect pipeline definitions
│   ├── tasks/            # Individual Prefect tasks
│   └── utils/            # Utility modules
├── main.py               # Entry point
├── requirements.txt      # Project dependencies
├── setup_ground_truth.py # Script to set up ground truth data
└── env.example           # Example environment variables
```

## Setup

1. Clone the repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Copy `env.example` to `.env` and update it with your Azure credentials:
   ```
   cp env.example .env
   ```
4. Set up your ground truth CSV file:
   ```
   ./setup_ground_truth.py
   ```

## Configuration

Set the following environment variables in your `.env` file:

- `AZURE_STORAGE_CONNECTION_STRING`: Azure Storage connection string
- `AZURE_SOURCE_CONTAINER_NAME`: Source container name containing labels.json files
- `AZURE_DESTINATION_CONTAINER_NAME`: Destination container for corrected labels.json files
- `GROUND_TRUTH_CSV_PATH`: Path to the ground truth CSV file

## Ground Truth Format

The ground truth CSV should have the following format:

```
R.P,Date,Rep,TRT,Path,L1,L2,L3,L4,L5,L6,L7,L8,L9,L10,L11,L12,L13,L14,L15,L16,L17,L18,L19,L20
1.1,,,PK,BR,95,-,-,35,90,-,-,95,40,85,45,60,-,80,-,-,-,-,50,40
1.1,,,PK,DM,85,95,-,95,-,85,55,15,-,80,55,25,10,20,20,10,-,35,-,55
...
```

Each row in the ground truth CSV represents the correct values for each measurement in the labels.json file. The system matches the rows based on the `R.P` and `Path` values.

## Labels.json Format

The labels.json files are expected to have a structure like:

```json
{
  "$schema": "https://schema.cognitiveservices.azure.com/formrecognizer/2021-03-01/labels.json",
  "document": "TEST_20250605_R8P3_R9P1.pdf",
  "labels": [
    {
      "label": "dynamic/0/R.P",
      "value": [
        {
          "page": 1,
          "text": "1.1",
          "boundingBoxes": [...]
        }
      ]
    },
    {
      "label": "dynamic/0/Path",
      "value": [
        {
          "page": 1,
          "text": "BR",
          "boundingBoxes": [...]
        }
      ]
    },
    {
      "label": "dynamic/0/L1",
      "value": [
        {
          "page": 1,
          "text": "95",
          "boundingBoxes": [...]
        }
      ]
    },
    ...
  ]
}
```

The processor matches each `dynamic/{index}/L{number}` label with the corresponding value in the ground truth CSV based on its `R.P` and `Path` values.

## Usage

Run the main pipeline:

```
python main.py
```

With optional parameters:

```
python main.py --prefix folder/subfolder/ --ground-truth path/to/ground_truth.csv
```

To test the label correction without running the full pipeline:

```
./test_correction.py --input your_labels.json --output corrected_labels.json
```

## Pipeline Workflow

1. List all labels.json files in the Azure source container
2. For each file:
   - Download the file
   - Save a local copy
   - Correct labels using ground truth data
   - Validate the structure of corrected data
   - Upload the corrected file to the destination container

## License

MIT 