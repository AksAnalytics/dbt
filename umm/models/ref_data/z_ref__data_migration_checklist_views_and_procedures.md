# UMM NA Migration Checklist for Views and Stored Procedures in `REF_DATA` schema

## Views

No Views

## Stored Procedures

| Object                                                                  | Description                                                                                                                                           |
| ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| p\_build\_hfmfxrates\_current                                           | Loads the ref\_data.hfmfxrates\_current table from the bods.hfm\_vw\_hfm\_actual\_trans\_current source table                                         |
| p\_build\_ref\_data\_data\_processing\_rule                             | Loads the SGM and AGM rules into the ref\_data.data\_processing\_rule table                                                                           |
| p\_build\_ref\_data\_data\_processing\_rule\_agm                        | Loads the AGM rules into the ref\_data.data\_processing\_rule\_agm table                                                                              |
| p\_build\_ref\_data\_fob\_soldto\_barcust\_map                          | Loads the ref\_data.fob\_soldto\_barcust\_mapping table manually using INSERT statements                                                              |
| p\_build\_ref\_data\_pnl\_acct                                          | Loads the ref\_data.pnl\_acct table manually using INSERT statements                                                                                  |
| p\_build\_ref\_data\_pnl\_acct\_agm                                     | Loads the ref\_data.pnl\_acct\_agm table manually using INSERT statements                                                                             |
| p\_build\_ref\_data\_ptg\_accruals\_agm                                 | Loads the ref\_data.ptg\_accruals table from the bods.c11\_0ec\_pca3\_current source table                                                            |
| p\_build\_ref\_data\_volume\_conv\_sku                                  | Loads the ref\_data.volume\_conv\_sku table manually using INSERT statements                                                                          |
| p\_build\_ref\_entity                                                   | Loads the ref\_data.entity table manually using INSERT statements (will soon be updated to use the bods.drm\_entity\_current source table)            |
| p\_build\_reference\_agm\_bnr\_financials\_extract                      | Loads the ref\_data.agm\_bnr\_financials\_extract table from the various Hyperion source files available via S3                                       |
| p\_build\_reference\_calendar                                           | Loads the ref\_data.calendar table from a source calendar file available via S3                                                                       |
| p\_build\_reference\_customer\_commercial\_hierarchy                    | Loads the ref\_data.customer\_commercial\_hierarchy table from a source file available via S3                                                         |
| p\_build\_reference\_demand\_group\_to\_bar\_customer\_mapping          | Loads the ref\_data.demand\_group\_to\_bar\_customer\_mapping table from a source file available via S3                                               |
| p\_build\_reference\_entity\_to\_plant\_to\_division\_to\_ssbu\_mapping | Loads the ref\_data.entity\_to\_plant\_to\_division\_to\_ssbu\_mapping table from a source file available via S3                                      |
| p\_build\_reference\_parent\_product\_hierarchy\_allocation             | Loads the ref\_data.parent\_product\_hierarchy\_allocation\_mapping table manually using INSERT statements                                            |
| p\_build\_reference\_product\_commercial\_hierarchy                     | Loads the ref\_data.product\_commercial\_hierarchy table from a source file available via S3                                                          |
| p\_build\_reference\_product\_hierarchy\_allocation                     | Loads the ref\_data.product\_hierarchy\_allocation\_mapping from two source files available via S3                                                    |
| p\_build\_reference\_rsa\_bible                                         | Loads the ref\_data.rsa\_bible table from a source file file available via S3                                                                         |
| p\_build\_sku\_barbrand\_mapping                                        | Loads the ref\_data.sku\_barbrand\_mapping table from the bods.c11\_0ec\_pca3\_current, sapc11.mara\_current, and sapc11.t023t\_current source tables |
| p\_build\_sku\_barbrand\_mapping\_sgm                                   | Loads the ref\_data.sku\_barbrand\_mapping\_sgm table based on existing fact data in the dw.fact\_pnl\_commercial\_stacked table                      |
| p\_build\_sku\_barproduct\_mapping                                      | Loads the ref\_data.sku\_barproduct\_mapping table from the bods.c11\_0material\_attr\_current source table                                           |
| p\_build\_sku\_barproduct\_mapping\_c11\_bods                           | Loads the ref\_data.sku\_barproduct\_mapping\_c11\_bods table from the bods.c11\_0ec\_pca3\_current source table                                      |
| p\_build\_sku\_barproduct\_mapping\_lawson\_bods                        | Loads the ref\_data.sku\_barproduct\_mapping\_lawson\_bods table from the bods.lawson\_mac\_pl\_trans\_current source table                           |
| p\_build\_sku\_barproduct\_mapping\_p10\_bods                           | Loads the ref\_data.sku\_barproduct\_mapping\_p10\_bods table from the bods.p10\_0ec\_pca\_3\_trans\_current source table                             |
| p\_build\_sku\_brand\_mapping\_masterdata                               | Loads the ref\_data.sku\_brand\_mapping\_masterdata table from the sapc11.mara\_current and sapc11.t023t\_current brnd source tables                  |
| p\_build\_sku\_gpp\_mapping                                             | Loads the ref\_data.sku\_gpp\_mapping table from the sapc11.mara\_current source table                                                                |
| p\_build\_sku\_gpp\_mapping\_sgm                                        | Loads the ref\_data.sku\_gpp\_mapping\_sgm table based on existing fact data in the dw.fact\_pnl\_commercial\_stacked table                           |
| p\_build\_soldto\_barcust\_mapping                                      | Loads the ref\_data.soldto\_barcust\_mapping table from the bods.c11\_0ec\_pca3\_current source table                                                 |
| p\_build\_soldto\_shipto\_barcust\_mapping                              | Loads the ref\_data.soldto\_shipto\_barcust\_mapping table from the bods.c11\_0ec\_pca3\_current source table                                         |
| umm\_closedate\_config                                                  | Loads the ref\_data.umm\_closedate\_config table based on date logic applied to the ref\_data.calendar table                                          |**
