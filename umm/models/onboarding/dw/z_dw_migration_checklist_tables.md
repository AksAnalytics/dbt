# UMM NA Migration Checklist for Tables in `DW` schema

## Tables and Views

| Object                         | Description                                                                                                                          |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| fact\_pnl\_commercial          | Primary fact table for SGM financial transactions; includes amounts for various BA&R accounts as columns across a single transaction |
| fact\_pnl\_commercial\_stacked | Fact table for SGM financial transactions where each row represents a transaction and an amount for a single BA&R account            |
| fact\_pnl\_ocos                | Primary fact table for AGM financial transactions; includes amounts for various BA&R accounts as columns across a single transaction |
| fact\_pnl\_ocos\_stacked       | Fact table for AGM financial transactions where each row represents a transaction and an amount for a single BA&R account            |
| dim\_business\_unit            | Dimension table for business units; based on the BA&R entities                                                                       |
| dim\_currency                  | Dimension table for currency codes and formats                                                                                       |
| dim\_customer                  | Dimension table for SBD customers; includes "sold to" and "ship to" details along with various hierarchy attributes                  |
| dim\_customer\_restatement     | Utility table used for certain "sold to" customer restatements                                                                       |
| dim\_dataprocessing\_outcome   | Dimension table used to tag fact records based on the outcome of allocation engine rules and phases                                  |
| dim\_dataprocessing\_rule      | Dimension table used to tag fact records with the allocation engine rule applied to each row                                         |
| dim\_date                      | Utility table; includes calendar/fiscal date parts as well as other date-level attributes                                            |
| dim\_product                   | Dimension table for SBD products; includes BA&R product identifiers along with SKU, brand, and portfolio attributes and hierarhies   |
| dim\_product\_restatement      | Utility table used for certain product restatements                                                                                  |
| dim\_scenario                  | Dimension table for financial scenarios (actual, budgeted, and forecast)                                                             |
| dim\_source\_system            | Dimension table for data source systems                                                                                              |
| dim\_transactional\_attributes |                                                                                                                                      |