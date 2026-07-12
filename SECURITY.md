This page is adapted from the Zcash Foundation's security policy for Zebra (copyright Zcash Foundation, 2021), which Zakura is a fork of. It is posted in order to conform to this standard: <https://github.com/RD-Crypto-Spec/Responsible-Disclosure/tree/d47a5a3dafa5942c8849a93441745fdd186731e6>

# Security Disclosures

## Disclosure Principles

Zakura's security disclosure process aims to achieve the following goals:

- protecting Zcash users and the wider Zcash ecosystem
- respecting the work of security researchers
- improving the ongoing health of the Zcash ecosystem

Specifically, we will:

- assume good faith from researchers and ecosystem partners
- operate a no fault process, focusing on the technical issues
- work with security researchers, regardless of how they choose to disclose issues

## Receiving Disclosures

The Zakura team is committed to working with researchers who submit security vulnerability notifications to us to resolve those issues on an appropriate timeline and perform a coordinated release, giving credit to the reporter if they would like.

Please submit issues privately through GitHub's security advisory reporting for this repository: <https://github.com/zakura-core/zakura/security/advisories/new>.

If an issue also affects upstream [Zebra](https://github.com/ZcashFoundation/zebra), please also report it to the Zcash Foundation by following [upstream's security policy](https://github.com/ZcashFoundation/zebra/blob/main/SECURITY.md). Issues in Zakura's own additions (for example the Zakura P2P v2 stack) should be reported only to us.

## Sending Disclosures

In the case where we become aware of security issues affecting other projects that has never affected Zakura or Zcash, our intention is to inform those projects of security issues on a best effort basis.

In the case where we fix a security issue in Zakura or Zcash that also affects the following neighboring projects, our intention is to engage in responsible disclosures with them as described in <https://github.com/RD-Crypto-Spec/Responsible-Disclosure>, subject to the deviations described in the section at the bottom of this document.

## Responsible Disclosure to Neighboring Projects

We intend to engage in responsible disclosures for security issues affecting Zakura or Zcash technology with the following contacts, subject to the deviations described in the next section:

- The Zcash Foundation (Zebra, which Zakura is a fork of) - per <https://github.com/ZcashFoundation/zebra/blob/main/SECURITY.md>
- The Electric Coin Company - <security@z.cash> via PGP

## Deviations from the Standard

### Monetary Base Protection

Zcash is a technology that provides strong privacy. Notes are encrypted to their destination, and then the monetary base is kept via zero-knowledge proofs intended to only be creatable by the real holder of Zcash. If this fails, and a counterfeiting bug results, that counterfeiting bug might be exploited without any way for blockchain analyzers to identify the perpetrator or which data in the blockchain has been used to exploit the bug. Rollbacks before that point, such as have been executed in some other projects in such cases, are therefore impossible.

The standard describes reporters of vulnerabilities including full details of an issue, in order to reproduce it. This is necessary for instance in the case of an external researcher both demonstrating and proving that there really is a security issue, and that security issue really has the impact that they say it has - allowing the development team to accurately prioritize and resolve the issue.

In the case of a counterfeiting bug, we might decide not to include those details with our reports to partners ahead of coordinated release, so long as we are sure that they are vulnerable.
