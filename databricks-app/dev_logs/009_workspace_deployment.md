# Development Log 009: Databricks Workspace Deployment (dev_metaflow)

## Enactment Summary
- **Component**: Databricks App Deployment & Startup in Workspace
- **Target Workspace**: `dev_metaflow` (`https://dbc-2f6b7d4f-8c5b.cloud.databricks.com`)
- **Status**: Live and Running (`RUNNING` / `ACTIVE` / `SUCCEEDED`)

## Details of Enacted Actions
1. **Bundle App Resource Configuration**:
   - Cleaned app resource in `resources/metaflow_onboarding_app.yml`.
   - Validated bundle target configuration with `databricks bundle validate -t dev_metaflow`.
2. **Selective Resource Deployment**:
   - Deployed only the `metaflow_onboarding_app` resource via DABs:
     ```powershell
     databricks bundle deploy -t dev_metaflow --select apps.metaflow_onboarding_app --auto-approve
     ```
   - Synced source code to `/Workspace/Users/metaflow@nrmanalytix.com/.bundle/NextGen_Metadata_Framework/dev_metaflow/files/metaflow-onboarding-app`.
3. **App Start & Source Deployment**:
   - Started serverless compute:
     ```powershell
     databricks apps start metaflow-onboarding -p dev_metaflow
     ```
   - Deployed source code snapshot:
     ```powershell
     databricks apps deploy metaflow-onboarding --source-code-path /Workspace/Users/metaflow@nrmanalytix.com/.bundle/NextGen_Metadata_Framework/dev_metaflow/files/metaflow-onboarding-app -p dev_metaflow
     ```
4. **Verification & Status**:
   - **App Name**: `metaflow-onboarding`
   - **App Status**: `RUNNING`
   - **Compute Status**: `ACTIVE`
   - **Deployment Status**: `SUCCEEDED` ("App started successfully")
   - **Live URL**: `https://metaflow-onboarding-7474645419981263.aws.databricksapps.com`
