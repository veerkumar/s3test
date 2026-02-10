# S3 Test Results: AWS S3 vs Custom Endpoint (10.42.236.12)

Comparison of test results run with **no quirks** (conditional PUT quirks removed).

| Test | Custom (10.42.236.12) | AWS S3 |
|------|------------------------|--------|
| **ConditionalRequestTests** | | |
| thatConditionalPutIfMatchEtagWorks | ✅ (after fix) | ✅ |
| thatConditionalPutIfNoneMatchEtagWorks | ❌ expected 412, got 501 | ❌ expected 412, got 501 |
| thatConditionalPutIfNoneMatchStarWorks | ✅ | ✅ |
| **DeleteObjectsTests** | | |
| testDeleteObjectsWithDifferentETag | ❌ (server deleted despite wrong ETag) | ✅ (server respected ETag) |
| testDeleteObjectsWithMatchingETag | ✅ | ✅ |
| testDeleteObjectsContainingDotDot | ✅ | ✅ |
| testDeleteObjectsByKey | ✅ | ✅ |
| **MultiPartUploadTests** | | |
| thatMultipartRetrievesOriginalParts | ❌ (part count / HEAD part not supported) | ✅ |
| **ObjectKeyTests** | | |
| testSurrogatePairsAreRejected | ❌ (server accepted invalid UTF-8) | ✅ (server rejected) |
| testOverlongEncodingsAreRejected | ❌ (server accepted) | ✅ (server rejected) |
| testOverlongNullIsRejected | ❌ (server accepted) | ✅ (server rejected) |
| testCodePointMinIsAccepted | ✅ | ✅ |
| testKeyNamesWithHighCodePointsAreAccepted | ✅ | ✅ |
| testNullIsRejected | ✅ | ✅ |
| testSimpleObjectKeySigning | ✅ | ✅ |
| testThatServerDoesNotNormalizeCodePoints | ✅ | ✅ |
| **Others (Checksum, DeleteObject, GetObject, ListBuckets, ListObjects, PrefixDelimiter)** | ✅ | ✅ (through testNullPrefix; run timed out before PutObjectTests) |

---

## Summary of differences

1. **thatConditionalPutIfNoneMatchEtagWorks**  
   **Same on both:** AWS and your endpoint return **501** for If-None-Match with an ETag. The test expects **412**. So neither matches the test’s expectation; this is a test/expectation issue or an optional feature.

2. **testDeleteObjectsWithDifferentETag**  
   **Custom:** Deletes the object even when the request ETag is wrong (conditional delete not enforced).  
   **AWS:** Does not delete when ETag does not match (conditional delete enforced).  
   **Gap:** Custom endpoint does not enforce optional ETag on DeleteObjects.

3. **thatMultipartRetrievesOriginalParts**  
   **Custom:** Fails (HEAD with part number or part count not supported or not as expected).  
   **AWS:** Passes (full multipart and part metadata support).  
   **Gap:** Custom endpoint is missing or differing multipart metadata (e.g. `partsCount`, HEAD by part number).

4. **testSurrogatePairsAreRejected / testOverlongEncodingsAreRejected / testOverlongNullIsRejected**  
   **Custom:** Accepts invalid UTF-8 keys (returns 2xx).  
   **AWS:** Rejects them (4xx/5xx).  
   **Gap:** Custom endpoint does not validate object keys for invalid UTF-8 (surrogates, overlong encodings).

---

## Conclusion

- **Conditional PUT (If-Match):** Both support it; the test fix made the behavior consistent with expectations.  
- **Conditional PUT (If-None-Match with ETag):** Both return 501; test expects 412.  
- **DeleteObjects with ETag:** Only AWS enforces the condition; custom endpoint should be updated to honor ETag when provided.  
- **Multipart metadata:** Only AWS passes; custom endpoint needs HEAD/GET part number and correct part count behavior.  
- **UTF-8 key validation:** Only AWS rejects invalid UTF-8 keys; custom endpoint should reject surrogate and overlong encodings for S3 compatibility.

*Note: The AWS run was stopped by a timeout during PrefixDelimiterTests; all tests up to that point completed. PutObjectTests were not included in the AWS run due to the timeout.*
