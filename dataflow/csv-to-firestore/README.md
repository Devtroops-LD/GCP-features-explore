# CSV to Firestore (Java Dataflow Flex Template)

Apache Beam (Java 17, Beam 2.60.0) pipeline that reads a CSV file from Cloud Storage and upserts every row as a document in a Firestore collection.

- The CSV header row defines the document fields. Integers and decimals are stored as numbers, empty cells as `null`, everything else as strings.
- The `keyField` column (default `id`) becomes the Firestore document ID, so re-running the job overwrites documents instead of duplicating them.
- Writes go through Beam's `FirestoreIO.v1().write().batchWrite()`, which batches, retries and throttles requests.

CI in this repo ([`.github/workflows/csv-to-firestore-ci.yml`](../../.github/workflows/csv-to-firestore-ci.yml)) builds and tests on every pull request and push. After a green build on `main` it triggers the deploy workflow in [gcp-cicd](https://github.com/Devtroops-LD/gcp-cicd), which owns all GCP setup, IAM and deployment.

## Layout

| Path | Purpose |
| --- | --- |
| `src/main/java/.../CsvToFirestore.java` | Entry point, pipeline options, header reading |
| `src/main/java/.../CsvLinesToFirestoreWrites.java` | Transform from CSV lines to Firestore `Write`s |
| `src/main/java/.../FirestoreValues.java` | CSV text to Firestore `Value` conversion |
| `Dockerfile` | Flex Template launcher image (copies the shaded jar) |
| `metadata.json` | Flex Template parameters |
| `sample-data/employees.csv` | 20-row sample input |

## Pipeline parameters

| Parameter | Required | Default |
| --- | --- | --- |
| `input` | yes | |
| `collection` | yes | |
| `keyField` | no | `id` |
| `firestoreProject` | no | job project |
| `firestoreDb` | no | `(default)` |

## Build and test

```bash
cd dataflow/csv-to-firestore
mvn -B package          # runs unit tests and builds target/csv-to-firestore-bundled.jar
```

## Run locally (DirectRunner) against real Firestore

```bash
gcloud auth application-default login
java -jar target/csv-to-firestore-bundled.jar \
  --runner=DirectRunner \
  --project=devtroops-ld \
  --input=sample-data/employees.csv \
  --collection=employees
```

## Run on Dataflow manually

Once the gcp-cicd workflow has published the template (normally you just tick **run_job** on that workflow instead):

```bash
PROJECT_ID=devtroops-ld
REGION=us-central1
BUCKET=gs://${PROJECT_ID}-dataflow

gcloud dataflow flex-template run "csv-to-firestore-$(date +%Y%m%d-%H%M%S)" \
  --project=${PROJECT_ID} \
  --region=${REGION} \
  --template-file-gcs-location=${BUCKET}/templates/csv-to-firestore.json \
  --service-account-email=csv-to-firestore-worker@${PROJECT_ID}.iam.gserviceaccount.com \
  --temp-location=${BUCKET}/temp \
  --staging-location=${BUCKET}/staging \
  --max-workers=1 \
  --worker-machine-type=e2-standard-2 \
  --parameters=input=${BUCKET}/input/employees.csv \
  --parameters=collection=employees
```
