# Description of Failed Tests and Why They Failed

This document describes each test that failed when running the S3 test suite against the endpoint (with no quirks configured) and the reason for each failure.

---

## 1. **thatConditionalPutIfMatchEtagWorks** (ConditionalRequestTests)

**What the test does:**  
Puts an object `"object"` with content `"hello"`, then overwrites it with content `"bar"` using **If-Match: &lt;current-etag&gt;** (the ETag from the first PUT). The precondition is satisfied, so the overwrite is expected to succeed and the object should become `"bar"`.

**Why it failed:**  
The test has a **bug in the success path**. When the overwrite succeeds (no exception), it asserts that the content is `"bar"` and that the ETags match, then it **always** calls `fail("PutObject using 'If-Match: \"<etag>\"' should fail if object etag does not match")`. So any successful overwrite is reported as a failure. The server behaved correctly (it allowed the overwrite when If-Match matched); the failure is due to this erroneous `fail()` in the test code.

---

## 2. **thatConditionalPutIfNoneMatchEtagWorks** (ConditionalRequestTests)

**What the test does:**  
Puts an object, then attempts to overwrite it with **If-None-Match: &lt;current-etag&gt;** (meaning “only write if the current ETag is different”). Since the object already has that ETag, the overwrite should be rejected with **412 Precondition Failed**.

**Why it failed:**  
The test expects HTTP **412** (Precondition Failed). The server returned **501** (Not Implemented). So the endpoint does not support the **If-None-Match** conditional with an ETag value and reports the feature as not implemented instead of returning 412.

---

## 3. **testDeleteObjectsWithDifferentETag** (DeleteObjectsTests)

**What the test does:**  
Creates an object `"a"` with content `"Hello"`, then calls **DeleteObjects** for key `"a"` with a **wrong ETag** (`"foo"`). The test expects that the object is **not** deleted (conditional delete): `hasDeleted()` should be false and the deleted list should be empty.

**Why it failed:**  
The assertion failed: either `hasDeleted()` was true or the deleted list was non-empty. So the server **deleted the object even though the ETag did not match**. The S3 endpoint does not enforce the optional ETag condition on DeleteObjects; it deletes the object regardless of the supplied ETag.

---

## 4. **thatMultipartRetrievesOriginalParts** (MultiPartUploadTests)

**What the test does:**  
Performs a **multipart upload** with 11 parts (sizes from 3 MB to 13 MB, each ≥ 5 MB where required), completes the upload, then uses **HEAD Object** with `partNumber(1)` to get metadata. It checks that `partsCount()` equals 11 (or that the quirk `MULTIPART_SIZES_NOT_KEPT` or `GET_OBJECT_PARTCOUNT_NOT_SUPPORTED` applies), then either fetches by part number or as a single object and verifies total size matches.

**Why it failed:**  
The failure is an **AssertionError** at the multipart-metadata checks (around lines 97–104). Most likely:

- **HEAD Object** with `partNumber(1)` either is not supported (e.g. 4xx/5xx), or  
- **partsCount()** is null or not 11.

So the server either does not support the part-number parameter on HEAD/GET, or does not report part count. The test then cannot validate part count or per-part sizes and fails the corresponding assertions.

---

## 5. **testSurrogatePairsAreRejected** (ObjectKeyTests)

**What the test does:**  
Sends a **PUT** with an object key that contains **invalid UTF-8**: the key includes the raw UTF-8 encoding of surrogate code units (high and low surrogates) instead of the single UTF-8 sequence for the corresponding supplementary character. Per Unicode/UTF-8, such surrogate-byte sequences in UTF-8 are invalid. The test expects the server to **reject** the request with an HTTP status in the **4xx or 5xx** range.

**Why it failed:**  
The assertion `status / 100 > 3` failed, so the server returned a **2xx** status and **accepted** the key. The endpoint does not validate UTF-8 key encoding and allows keys that contain invalid UTF-8 (surrogate pairs encoded as separate surrogate code units in the byte stream).

---

## 6. **testOverlongEncodingsAreRejected** (ObjectKeyTests)

**What the test does:**  
Sends a **PUT** with an object key that contains an **overlong UTF-8 encoding**: the character `'a'` encoded in 4 bytes instead of 1. Overlong encodings are invalid in UTF-8. The test expects the server to **reject** the request with **4xx or 5xx**.

**Why it failed:**  
The server returned **2xx** and accepted the key. The endpoint does not reject overlong UTF-8 sequences in object keys.

---

## 7. **testOverlongNullIsRejected** (ObjectKeyTests)

**What the test does:**  
Sends a **PUT** with an object key that contains the **overlong encoding of the null character** (bytes `0xC0 0x80` instead of `0x00`). This is invalid UTF-8. The test expects **4xx or 5xx**.

**Why it failed:**  
The server returned **2xx** and accepted the key. The endpoint does not reject this invalid UTF-8 encoding in keys.

---

## Summary Table

| Test | Cause of failure |
|------|------------------|
| **thatConditionalPutIfMatchEtagWorks** | Test bug: `fail()` called on success path; server behavior is correct. |
| **thatConditionalPutIfNoneMatchEtagWorks** | Server returns 501 (Not Implemented) instead of 412 for If-None-Match with ETag. |
| **testDeleteObjectsWithDifferentETag** | Server ignores ETag on DeleteObjects and deletes the object anyway. |
| **thatMultipartRetrievesOriginalParts** | HEAD/GET with part number or parts count not supported or not as expected. |
| **testSurrogatePairsAreRejected** | Server accepts invalid UTF-8 (surrogate bytes) in keys (2xx instead of 4xx/5xx). |
| **testOverlongEncodingsAreRejected** | Server accepts overlong UTF-8 encoding in keys (2xx instead of 4xx/5xx). |
| **testOverlongNullIsRejected** | Server accepts overlong null encoding in keys (2xx instead of 4xx/5xx). |
