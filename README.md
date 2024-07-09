# omicidx-gh-etl

## ETL status badges

| Workflow | Description |
|-------|------| 
| [![Biosample](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/ncbi_biosample.yaml/badge.svg)](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/ncbi_biosample.yaml) | All metadata from the NCBI Biosample and Bioproject databases |
| [![PubMed](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/pubmed_etl.yaml/badge.svg)](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/pubmed_etl.yaml) | PubMed metadata and abstracts |
| [![icite](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/icite.yaml/badge.svg)](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/icite.yaml) | NIH iCite resource for article impact and citations |
| [![GEO](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/geo.yaml/badge.svg)](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/geo.yaml) | All metadata from NCBI Gene Expression Omnibus (GEO) |
| [![SRA](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/ncbi_sra_etl.yaml/badge.svg)](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/ncbi_sra_etl.yaml) | All metadata from NCBI Sequence Read Archive (SRA) |
| [![Scimago](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/scimago.yaml/badge.svg)](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/scimago.yaml) | the Scimago Journal Impact Factor database | 


## Infrastructure setup

We are using GCP to host the cloud storage associate with this project. 
```
# TODO: replace ${PROJECT_ID} with your value below.
export PROJECT_ID=omicidx-338300
export GITHUB_ORG=seandavi
export GITHUB_REPO=omicidx-gh-etl

gcloud iam workload-identity-pools create "github-ip" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --display-name="GitHub Actions Pool"

gcloud iam workload-identity-pools describe "github-ip" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --format="value(name)"


# TODO: replace ${PROJECT_ID} and ${GITHUB_ORG} with your values below.

gcloud iam workload-identity-pools providers create-oidc "${GITHUB_REPO}" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --workload-identity-pool="github-ip" \
  --display-name="My GitHub repo Provider" \
  --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository,attribute.repository_owner=assertion.repository_owner" \
  --attribute-condition="assertion.repository_owner == '${GITHUB_ORG}'" \
  --issuer-uri="https://token.actions.githubusercontent.com"

# TODO: replace ${PROJECT_ID} with your value below.

gcloud iam workload-identity-pools providers describe "${GITHUB_REPO}" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --workload-identity-pool="github-ip" \
  --format="value(name)"
```


```
gcloud projects add-iam-policy-binding omicidx-338300 \
  --role="roles/storage.Admin" \
  --member="principalSet://iam.googleapis.com/projects/492900567997/locations/global/workloadIdentityPools/github-ip/attribute.repository/seandavi/omicidx-gh-etl" 


# or with repository owner (see above for the "conditions")
gcloud projects add-iam-policy-binding omicidx-338300 \
  --member="principalSet://iam.googleapis.com/projects/390395656114/locations/global/workloadIdentityPools/github/attribute.repository_owner/seandavi" \
  --role="roles/storage.Admin"   

```
