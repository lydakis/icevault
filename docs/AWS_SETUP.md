# AWS Setup Guide

IceVault supports two authentication methods. We recommend **SSO (IAM Identity Center)** for best security practices, but static access keys work fine for personal use.

---

## Option A: IAM Identity Center / SSO (Recommended)

Temporary credentials that auto-expire. No long-lived secrets on disk.

### Step 1: Enable IAM Identity Center

1. Sign in to the [AWS Console](https://console.aws.amazon.com/) with your root or admin account
2. Search for **"IAM Identity Center"** (formerly AWS SSO) and open it
3. If prompted, click **"Enable"**
4. Choose **"Organization instance"** (recommended) — this works even for a single account
5. For identity source, keep the default: **"IAM Identity Center directory"** (simplest for personal use)

### Step 2: Create a User

1. In IAM Identity Center → **Users** → **Add user**
2. Fill in: username (e.g. `george`), email, first/last name
3. You'll receive an email to set your password and activate the account

### Step 3: Create a Permission Set

1. Go to **Permission sets** → **Create permission set**
2. Choose **"Custom permission set"**
3. Name it: `IceVaultBackup`
4. Session duration: **12 hours** (maximum for CLI use)
5. Add an **inline policy** with this JSON:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "IceVaultBackup",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "s3:CreateMultipartUpload",
                "s3:UploadPart",
                "s3:CompleteMultipartUpload",
                "s3:AbortMultipartUpload"
            ],
            "Resource": [
                "arn:aws:s3:::YOUR-BUCKET-NAME",
                "arn:aws:s3:::YOUR-BUCKET-NAME/*"
            ]
        }
    ]
}
```

> Replace `YOUR-BUCKET-NAME` with your actual bucket name.

### Step 4: Assign the Permission Set

1. Go to **AWS accounts** in IAM Identity Center
2. Select your AWS account → **Assign users or groups**
3. Select your user → select the `IceVaultBackup` permission set → **Submit**

### Step 5: Create an S3 Bucket

1. Go to [S3 Console](https://console.aws.amazon.com/s3/) → **Create bucket**
2. Name: e.g. `icevault-george-backup` (must be globally unique)
3. Region: pick one close to you (e.g. `us-east-1`)
4. Keep defaults (block all public access ✅, bucket versioning off)
5. Click **Create bucket**

### Step 6: Configure the AWS CLI

```bash
aws configure sso --profile icevault
```

You'll be prompted for:

| Prompt | What to enter |
|--------|---------------|
| SSO session name | `icevault` |
| SSO start URL | Your AWS access portal URL (find it in IAM Identity Center → Settings → "AWS access portal URL", looks like `https://d-xxxxxxxxxx.awsapps.com/start`) |
| SSO region | Region where you enabled Identity Center (e.g. `us-east-1`) |
| SSO registration scopes | Just press Enter (default is fine) |

A browser window will open — log in with the user you created in Step 2. Then back in the terminal:

| Prompt | What to enter |
|--------|---------------|
| Account | Select your AWS account |
| Role | Select `IceVaultBackup` |
| CLI default client Region | e.g. `us-east-1` |
| CLI default output format | Press Enter (default) |
| CLI profile name | `icevault` |

### Step 7: Test It

```bash
# Login (opens browser)
aws sso login --profile icevault

# Verify
aws sts get-caller-identity --profile icevault

# Test bucket access
aws s3 ls s3://YOUR-BUCKET-NAME --profile icevault
```

### Step 8: Configure IceVault

1. Open IceVault → Settings
2. Authentication Method: **SSO Profile**
3. Profile name: `icevault`
4. Bucket: your bucket name
5. Region: your bucket's region
6. Source: select your backup folder

### Token Expiry

SSO sessions last **8–12 hours** (depending on your permission set session duration). When they expire:

- IceVault will detect expired credentials and prompt you to re-login
- Run `aws sso login --profile icevault` or click "Login" in IceVault settings
- A browser window opens, you authenticate, and you're good for another session

> **Note for scheduled backups:** If you use LaunchAgent scheduling, SSO tokens may be expired when the backup triggers. For unattended backups, consider Option B (static keys) or set up a longer session duration.

---

## Option B: Static Access Keys (Simple)

Long-lived credentials. Easier to set up, works for unattended backups, but less secure.

### Step 1: Create an IAM User

1. Go to [IAM Console](https://console.aws.amazon.com/iam/) → **Users** → **Create user**
2. Name: `icevault-backup`
3. Do NOT check "Provide user access to the AWS Management Console"
4. Click **Next**

### Step 2: Attach a Policy

1. Choose **"Attach policies directly"**
2. Click **"Create policy"** → JSON tab → paste:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "IceVaultBackup",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "s3:CreateMultipartUpload",
                "s3:UploadPart",
                "s3:CompleteMultipartUpload",
                "s3:AbortMultipartUpload"
            ],
            "Resource": [
                "arn:aws:s3:::YOUR-BUCKET-NAME",
                "arn:aws:s3:::YOUR-BUCKET-NAME/*"
            ]
        }
    ]
}
```

3. Name it `IceVaultBackupPolicy` → Create
4. Back on the user creation page, attach this policy → **Create user**

### Step 3: Create Access Keys

1. Open the user → **Security credentials** tab
2. **Create access key** → choose "Command Line Interface (CLI)"
3. Save the **Access Key ID** and **Secret Access Key** (you won't see the secret again!)

### Step 4: Configure

**Option A — AWS CLI (recommended, IceVault auto-detects):**
```bash
aws configure --profile icevault
# Enter: access key, secret key, region, output format
```

**Option B — IceVault directly:**
1. Open IceVault → Settings
2. Authentication Method: **Static Keys**
3. Enter your access key and secret key (stored in macOS Keychain)

### Step 5: Create Bucket + Test

Same as SSO Steps 5 and 7 above.

---

## Which Should I Use?

| | SSO | Static Keys |
|---|---|---|
| **Security** | ✅ Temporary creds, auto-expire | ⚠️ Long-lived, rotate manually |
| **Unattended backups** | ⚠️ Needs re-auth every 8-12h | ✅ Works forever |
| **Setup complexity** | More steps | Fewer steps |
| **AWS recommendation** | ✅ Best practice | Acceptable for personal use |

**Our honest take:** For a personal backup app on your own Mac, static keys scoped to a single bucket are perfectly fine. SSO is the "right" way, but the practical risk of a scoped static key in your Keychain is very low. Pick whichever you're comfortable with.

For unattended scheduled backups (LaunchAgent running at 3am), static keys are more reliable since SSO tokens expire.

---

## Troubleshooting

**"The SSO session associated with this profile has expired"**
```bash
aws sso login --profile icevault
```

**"Access Denied" on PutObject**
- Check your policy has the correct bucket name (no typos)
- Make sure the policy is attached to your user/permission set
- Verify with: `aws s3 ls s3://YOUR-BUCKET-NAME --profile icevault`

**"Could not connect to the endpoint URL"**
- Check your region setting matches the bucket's region
- Run `aws configure list --profile icevault` to verify config
