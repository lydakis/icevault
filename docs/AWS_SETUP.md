# icevault + aws iam identity center (sso) setup guide (single-person, single-account)

this guide walks through setting up **aws iam identity center** (formerly “aws sso”) so **icevault** can back up to **one s3 bucket**, using **temporary credentials** (no long-lived access keys) and **least-privilege** permissions.

---

## 0) important reality check: you need an *organization instance* for aws account access

iam identity center has multiple “instance types.” if you want sso access **to your aws account** (and to use `aws configure sso` / `aws sso login` for role credentials), you need an **organization instance**. an **account instance** does *not* provide “multi-account permissions” or “aws access portal for single-sign on access to your aws accounts.” ([AWS Documentation][1])

for a “single account, single person” setup, this still works fine: you can enable an **organization instance** and keep the organization at **one account**. aws documentation explicitly describes enabling an organization instance as the recommended path, even if you started as a standalone account. ([AWS Documentation][2])

also: iam identity center for organizations can be enabled in **only one aws region**. if you later want to move it, you must delete the instance and create it again in the new region. ([AWS Documentation][2])

---

## 1) step-by-step: set up iam identity center (sso) for one personal aws account

### 1.1 enable iam identity center (organization instance)

1. sign in to the aws management console as:

   * root user (account owner), **or**
   * an iam user/role with administrative permissions. ([AWS Documentation][2])
2. open the **iam identity center** console.
3. choose the region you want as your “home” region for identity center.
4. under **Enable IAM Identity Center**, choose **Enable**. ([AWS Documentation][2])
5. on the **Enable IAM Identity Center with AWS Organizations** page, review and choose **Enable** to complete the process. ([AWS Documentation][2])

> note: the root account can be used to **enable** identity center, but you do **not** “use the root account as the sso identity.” you’ll create an identity center user next.

### 1.2 find (and optionally customize) your aws access portal url (start url)

you’ll need the **aws access portal url** for `aws configure sso`.

1. in the iam identity center console, go to **Dashboard**.
2. in **Settings summary**, locate your **AWS access portal URL**. ([AWS Documentation][3])

optional: you can customize the portal subdomain **once** (cannot be reversed):

* dashboard → settings summary → **Customize** under the aws access portal url. ([AWS Documentation][3])

### 1.3 create an identity center user (this is your “sso user”)

1. iam identity center console → **Users**
2. choose **Add user**
3. set:

   * **Username** (this is the username used to sign in to the aws access portal; cannot be changed later) ([AWS Documentation][4])
   * **Password**: easiest is “send email with password setup instructions” (default) ([AWS Documentation][4])
   * **Email address** (must be unique) ([AWS Documentation][4])
4. complete the wizard and choose **Add user**. ([AWS Documentation][4])

practical tip: create a group like `icevault-users` and put your user in it. it scales better and aws recommends assigning permissions to groups rather than individual users. ([AWS Documentation][5])

### 1.4 create a permission set for “icevault s3 only”

1. iam identity center console → under **Multi-account permissions** choose **Permission sets** ([AWS Documentation][6])
2. choose **Create permission set** ([AWS Documentation][6])
3. choose **Custom permission set** ([AWS Documentation][6])
4. attach **only an inline policy** (example policies are in section 5 below).
5. set **session duration**:

   * you can set the max session duration per permission set to **between 1 and 12 hours** (12h is usually best for backup runs). ([AWS Documentation][7])

### 1.5 assign your user (or group) to your aws account with that permission set

1. iam identity center console → **Multi-account permissions** → **AWS accounts** ([AWS Documentation][5])
2. select your account (in a single-account org, you’ll just see the one).
3. choose **Assign users or groups** ([AWS Documentation][5])
4. step 1: pick your **user** or **group** ([AWS Documentation][5])
5. step 2: pick the **permission set** you created ([AWS Documentation][5])
6. step 3: review and **Submit** ([AWS Documentation][5])

at this point, signing into the aws access portal should show your aws account + the role/permission set you assigned.

---

## 2) aws cli configuration: `aws configure sso --profile icevault`

### 2.1 prerequisite

use **aws cli v2** (identity center auth is a v2 feature).

### 2.2 run the command

```bash
aws configure sso --profile icevault
```

you’ll see prompts like:

```text
SSO session name (Recommended): icevault
SSO start URL [None]: https://YOUR-SUBDOMAIN.awsapps.com/start
SSO region [None]: us-east-1
SSO registration scopes [None]: sso:account:access
```

these prompts mean:

* **SSO session name (Recommended)**
  a local label for this sso session configuration. the cli writes an `[sso-session ...]` block in `~/.aws/config` and uses it for token caching and reuse across profiles. ([AWS Documentation][8])

* **SSO start URL**
  your **aws access portal url** (the one you found on the identity center dashboard). ([AWS Documentation][8])

* **SSO region**
  the region where your identity center directory/instance lives. ([AWS Documentation][8])

* **SSO registration scopes**
  typically `sso:account:access` for aws account access. ([AWS Documentation][8])

after that, the aws cli will attempt to open your browser so you can sign in, and may ask you to approve access for the cli (messages may mention `botocore` because the cli uses the aws sdk for python under the hood). ([AWS Documentation][8])

then the cli will prompt you to select:

* your aws account (you’ll likely have exactly one)
* the role name (this corresponds to the **permission set name** you created)

finally, your `~/.aws/config` ends up with something like:

```ini
[profile icevault]
sso_session = icevault
sso_account_id = 123456789011
sso_role_name = IceVaultS3Only
region = us-west-2
output = json

[sso-session icevault]
sso_region = us-east-1
sso_start_url = https://YOUR-SUBDOMAIN.awsapps.com/start
sso_registration_scopes = sso:account:access
```

(the `sso_session`, `sso_account_id`, and `sso_role_name` layout is the recommended modern configuration style.) ([AWS Documentation][8])

---

## 3) login flow: `aws sso login --profile icevault`

### 3.1 what happens (high level)

`aws sso login --profile icevault` performs an oauth2-based sign-in and caches an **authentication token** locally so future aws cli commands can fetch short-lived role credentials automatically. ([AWS Documentation][8])

### 3.2 what you see in the terminal + browser

**default (pkce) flow (aws cli v2.22.0+)**: the cli opens a browser on the same device.

you’ll see output like:

* “attempting to automatically open the sso authorization page…”
* a long `https://oidc.<region>.amazonaws.com/authorize?...` url if it can’t open the browser automatically. ([AWS Documentation][8])

**device-code flow (for headless/remote or older cli)**: you’ll see a url + a short code you type in:

```text
https://device.sso.us-west-2.amazonaws.com/
Then enter the code:
QCFK-N451
```

the aws cli used device auth by default before v2.22.0; on newer versions you can opt into it with `--use-device-code`. ([AWS Documentation][8])

### 3.3 token caching: where it lives and what gets cached

* the aws cli caches the **authentication token** to disk under `~/.aws/sso/cache`. ([AWS Documentation][8])
* the cache filename is based on your `sso_start_url` (and older/other wording in the docs also references the session name). ([AWS Documentation][8])

you can sign out and delete cached tokens with:

```bash
aws sso logout
```

([AWS Documentation][8])

### 3.4 expiration: the “8–12 hours” answer is… “it depends (in 3 layers)”

there are three separate clocks to understand:

1. **user interactive session (access portal session)**
   by default it lasts **8 hours**, but you can configure it from **15 minutes up to 90 days**. ([AWS Documentation][9])

2. **iam identity center oauth tokens (access token + refresh token)**
   when you authenticate, the cli receives a **refresh token** and **access token**. the cli checks the access token **hourly** and refreshes it using the refresh token; your overall session ends after the refresh token expires. ([AWS Documentation][10])

3. **aws role credentials session (permission set session duration)**
   the actual aws api credentials (what s3 sees) are short-lived and tied to the permission set’s session duration, which can be **1–12 hours**. ([AWS Documentation][7])
   the cli will automatically renew expired aws credentials as needed **as long as you’re still signed in and cached identity center credentials are valid**—otherwise you must log in again. ([AWS Documentation][8])

---

## 4) best practices for personal use: is iam identity center overkill?

honest take:

### identity center (sso) is great when:

* you want to avoid long-lived access keys (security win)
* you’re okay with browser sign-in occasionally
* you want a setup that feels closer to “modern aws” (temporary credentials everywhere)

### identity center can feel like overkill when:

* you just want a thing to run unattended for months
* you hate “console setup” and oauth flows
* you want maximum compatibility with every sdk/tool (static keys are universally supported)

### static iam user keys are pragmatic when:

* you need truly unattended scheduled backups
* you’re willing to treat the machine as a long-lived secret holder (store keys in macos keychain, rotate, monitor)

the pragmatic recommendation for icevault as a mac menu bar app:

* **support sso** as the default/safer option (great for interactive apps)
* **also support static keys** for people who need “set and forget” automation

---

## 5) least-privilege policy json (one bucket, s3 only, deep archive uploads)

below are two versions:

* **(A) minimal**: matches your exact requested actions (PutObject/GetObject/ListBucket) + deep archive enforcement
* **(B) practical**: adds the “real life” extras that many sdk uploaders need (`GetBucketLocation`, multipart cleanup)

> use the same json for:
>
> * an **identity center permission set inline policy**, and/or
> * an **iam user inline/managed policy** (static keys approach)

### placeholders used

* `YOUR_BUCKET` → your bucket name, e.g. `icevault-backups`
* optional: change `icevault/*` prefix if you want to scope backups to a folder-like prefix

---

### 5.A minimal policy (exact requested actions + enforce DEEP_ARCHIVE)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowListBucket",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::YOUR_BUCKET"
    },
    {
      "Sid": "AllowGetPutObjects",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": "arn:aws:s3:::YOUR_BUCKET/*"
    },
    {
      "Sid": "DenyObjectCreationIfNotDeepArchive",
      "Effect": "Deny",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::YOUR_BUCKET/*",
      "Condition": {
        "Bool": { "s3:ObjectCreationOperation": "true" },
        "StringNotEquals": { "s3:x-amz-storage-class": "DEEP_ARCHIVE" }
      }
    }
  ]
}
```

why the deny works:

* `s3:x-amz-storage-class` is a supported condition key for filtering by storage class ([AWS Documentation][11])
* `s3:ObjectCreationOperation` lets you target only operations that actually create an object (important for multipart flows) ([AWS Documentation][11])

---

### 5.B practical policy (recommended for real backup clients)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowBucketReadOnlyMetadata",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:ListBucketMultipartUploads"
      ],
      "Resource": "arn:aws:s3:::YOUR_BUCKET"
    },
    {
      "Sid": "AllowObjectReadWrite",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": "arn:aws:s3:::YOUR_BUCKET/*"
    },
    {
      "Sid": "DenyObjectCreationIfNotDeepArchive",
      "Effect": "Deny",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::YOUR_BUCKET/*",
      "Condition": {
        "Bool": { "s3:ObjectCreationOperation": "true" },
        "StringNotEquals": { "s3:x-amz-storage-class": "DEEP_ARCHIVE" }
      }
    },
    {
      "Sid": "DenyInsecureTransport",
      "Effect": "Deny",
      "Action": "s3:*",
      "Resource": [
        "arn:aws:s3:::YOUR_BUCKET",
        "arn:aws:s3:::YOUR_BUCKET/*"
      ],
      "Condition": { "Bool": { "aws:SecureTransport": "false" } }
    }
  ]
}
```

**icevault note:** if you *really* intend to restore data from deep archive, you will likely need `s3:RestoreObject` as well. (deep archive retrieval is a separate “restore then read” flow.)

---

## 6) token expiry + scheduled backups: how to make this not suck

### option 1: sso, with “long-ish” sessions (works well for a user-facing app)

this is the sweet spot for a menu bar app:

* set the **permission set session duration** to **12 hours** (max) so a long backup run doesn’t die mid-stream. ([AWS Documentation][7])
* set the **access portal (user interactive) session duration** longer than the default 8 hours—up to 90 days—so you don’t need to re-login constantly. ([AWS Documentation][9])
* rely on the cli/sdk token mechanism:

  * the cli gets an access token + refresh token
  * refreshes the access token hourly
  * you only need user interaction again when the refresh token/session expires. ([AWS Documentation][10])

**how icevault should handle expiry pragmatically:**

* if an s3 call fails due to expired sso, show a notification: “aws login required” with a button that runs `aws sso login --profile icevault` (or triggers an embedded browser login if you implement sso directly).
* don’t try to “silently” re-auth without user action—identity center is designed to require user sign-in when the session ends.

### option 2: static iam user keys (best for truly unattended backups)

if you want backups to run for months on a schedule without ever popping open a browser, this is where static keys are still the pragmatic choice.

if you do this:

* create a **dedicated iam user** (no console password), generate **one access key**, attach the least-privilege policy above.
* store the secret in **macos keychain** (or another secure store) and load it at runtime.
* rotate keys periodically (and provide a “re-auth” flow in icevault).

### option 3: “machine identity” alternatives (often overkill for personal mac apps)

things like roles anywhere, external secret brokers, etc. can avoid static keys *and* avoid frequent logins, but they’re typically more setup and more moving parts than most people want for personal backups.

---

## appendix: quick test commands

after you’ve done `aws sso login --profile icevault`, sanity check:

```bash
aws sts get-caller-identity --profile icevault
```

(the aws cli docs use this exact style of verification.) ([AWS Documentation][8])

and a deep archive upload test:

```bash
aws s3api put-object \
  --bucket YOUR_BUCKET \
  --key icevault/test.txt \
  --body ./test.txt \
  --storage-class DEEP_ARCHIVE \
  --profile icevault
```

---

### source docs used (for your own “why should I trust this?” auditing)

* enable identity center + region limitation ([AWS Documentation][2])
* instance types + account-instance limitations for aws account access ([AWS Documentation][1])
* add users (identity center directory) ([AWS Documentation][4])
* create permission set ([AWS Documentation][6])
* assign users/groups to aws accounts ([AWS Documentation][5])
* customize/find aws access portal url ([AWS Documentation][3])
* aws cli `aws configure sso`, pkce vs device-code, config file shape, token caching, auto-renew behavior ([AWS Documentation][8])
* aws cli token + refresh token behavior ([AWS Documentation][10])
* user interactive session duration default 8h, configurable up to 90d ([AWS Documentation][9])
* s3 condition keys used to enforce deep archive uploads ([AWS Documentation][11])

[1]: https://docs.aws.amazon.com/singlesignon/latest/userguide/identity-center-instances.html "https://docs.aws.amazon.com/singlesignon/latest/userguide/identity-center-instances.html"
[2]: https://docs.aws.amazon.com/singlesignon/latest/userguide/enable-identity-center.html "https://docs.aws.amazon.com/singlesignon/latest/userguide/enable-identity-center.html"
[3]: https://docs.aws.amazon.com/singlesignon/latest/userguide/howtochangeURL.html "https://docs.aws.amazon.com/singlesignon/latest/userguide/howtochangeURL.html"
[4]: https://docs.aws.amazon.com/singlesignon/latest/userguide/addusers.html "https://docs.aws.amazon.com/singlesignon/latest/userguide/addusers.html"
[5]: https://docs.aws.amazon.com/singlesignon/latest/userguide/assignusers.html "https://docs.aws.amazon.com/singlesignon/latest/userguide/assignusers.html"
[6]: https://docs.aws.amazon.com/singlesignon/latest/userguide/howtocreatepermissionset.html "https://docs.aws.amazon.com/singlesignon/latest/userguide/howtocreatepermissionset.html"
[7]: https://docs.aws.amazon.com/singlesignon/latest/userguide/howtosessionduration.html "https://docs.aws.amazon.com/singlesignon/latest/userguide/howtosessionduration.html"
[8]: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html "https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html"
[9]: https://docs.aws.amazon.com/singlesignon/latest/userguide/user-interactive-sessions.html "https://docs.aws.amazon.com/singlesignon/latest/userguide/user-interactive-sessions.html"
[10]: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso-concepts.html "https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso-concepts.html"
[11]: https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html "https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html"

