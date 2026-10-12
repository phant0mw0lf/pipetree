---
title: Notifications
description: Email a failed run through Azure Communication Services, with no connection string.
sidebar:
  order: 3
---

When a table fails, one email says which tables ran into errors and why. A run in which nothing failed sends nothing.

```yaml validate
systems:
  crm:
    type: synapse_link

notifications:
  email:
    endpoint: https://my-acs.communication.azure.com   # or {secret: acs-endpoint}
    sender: DoNotReply@0a1b2c3d-0000-0000-0000-000000000000.azurecomm.net
    to: [info@example.com]
    on: failure

bronze:
  tables:
    customer:
      source: {system: crm, object: account}
      business_key: [id]
      strategy: scd1
```

`endpoint`, `sender` and `to` are required. `on: failure` is the only value. Unknown keys are rejected. See the [YAML reference](../../reference/yaml/#notifications).

The console digest is still printed. A `notifier` passed to `run_pipeline` replaces both the console digest and the email.

## What the mail contains

The subject names the run, the counts and the first three failed tables:

```text
[pipetree] FAILED run 261010132021461: 1 failed, 2 upstream_failed - silver.dim_customer
```

The body comes as plain text and as simple HTML with no external resources:

```text
Run 261010132021461 FAILED

Tables: 1 failed, 2 upstream_failed, 2 succeeded
Tags: daily
Selection: customer (with dependents)
job_run_url: https://adb-1.azuredatabricks.net/jobs/7/runs/9

FAILED TABLES
* silver.dim_customer
  AnalysisException: column x not found
  attempts: 3
  upstream_failed because of it: gold.fact_sales

NOTES
* bronze.customer: 2 duplicate row(s) dropped
* bronze.orders: schema: added: channel (string)
* silver.report: input not refreshed: bronze.orders
```

- Per failed table: the error type, the error message (cut at 500 characters) and the attempts.
- `upstream_failed` tables are listed under the failed table that caused them.
- The notes of all tables, also those that succeeded. See [Notes and run log](../../concepts/notes-and-run-log/).
- The tags the run was limited to (`--tag`, `--exclude-tag`), the `--select` options of the run and the platform's run metadata (job and run link), when there are any.

## Set up without a secret

The email client signs in with a token, never with the connection string.

1. Create an Azure Communication Services resource, an Email Communication Service and a domain (an Azure-managed domain is enough). Connect the domain to the Communication Services resource. The sender address is on that domain.
2. Give the identity that sends the mail the role **Communication and Email Service Owner** on the Communication Services resource. Microsoft documents this built-in role for sending with Microsoft Entra authentication; for SMTP it also describes a custom role with `Microsoft.Communication/CommunicationServices/Read`, `CommunicationServices/Write` and `EmailServices/Write`. See [Set up SMTP authentication](https://learn.microsoft.com/en-us/azure/communication-services/quickstarts/email/send-email-smtp/smtp-authentication).
3. Provide the identity to the run:
   - **Databricks:** create a Unity Catalog service credential from an Access Connector and give its identity the role above. Grant `ACCESS` on the credential to the job's principal and register it for the audience `https://communication.azure.com` in `DatabricksPlatform(service_credentials={"https://communication.azure.com": "<credential name>"})`. See [Databricks](../databricks/).
   - **Fabric, a laptop, CI:** the client uses `DefaultAzureCredential`: `az login` locally, a managed identity or workload identity elsewhere. Fabric's `notebookutils.credentials.getToken` has no audience key for Communication Services.
4. Install the SDK: `pip install "pipetree[azure]"` (it brings `azure-communication-email`).

Without a service credential entry on Databricks, the platform falls back to `DefaultAzureCredential`.

## Failure isolation

A problem with the email never changes the run. A network error, a missing role, a missing SDK or a bad endpoint is logged as one `WARNING` that names the exception type, and the digest, the run log and the exit code stay as they are. The log has neither the message body nor any credential.

Sending waits at most 30 seconds for the service to confirm. Then the run goes on.
