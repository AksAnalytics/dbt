# UMM NA Migration Checklist for Tables in `REF_DATA` schema

## Tables and Views

| Object                                             | Description                                                                                                               |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| agm\_bnr\_financials\_extract                      | Reference table loaded with Hyperion financials extracts before model loading (allocations and fact table loading) begins |
| calendar                                           | Reference table loaded with calendar/fiscal date parts before model loading begins                                        |
| customer\_commercial\_hierarchy                    |                                                                                                                           |
| data\_processing\_rule                             | Reference table with allocation engine rules information                                                                  |
| data\_processing\_rule\_agm                        | Reference table with allocation engine rules information                                                                  |
| demand\_group\_to\_bar\_customer\_mapping          |                                                                                                                           |
| entity                                             | Reference table loaded with BA&R entity information before model loading begins                                           |
| entity\_to\_plant\_to\_division\_to\_ssbu\_mapping |                                                                                                                           |
| fob\_soldto\_barcust\_mapping                      |                                                                                                                           |
| hfmfxrates\_current                                |                                                                                                                           |
| parent\_product\_hierarchy\_allocation\_mapping    |                                                                                                                           |
| pnl\_acct                                          |                                                                                                                           |
| pnl\_acct\_agm                                     |                                                                                                                           |
| product\_commercial\_hierarchy                     |                                                                                                                           |
| product\_hierarchy\_allocation\_mapping            |                                                                                                                           |
| ptg\_accruals                                      |                                                                                                                           |
| rsa\_bible                                         |                                                                                                                           |
| sku\_barbrand\_mapping                             |                                                                                                                           |
| sku\_barbrand\_mapping\_sgm                        |                                                                                                                           |
| sku\_barproduct\_mapping                           |                                                                                                                           |
| sku\_barproduct\_mapping\_c11\_bods                |                                                                                                                           |
| sku\_barproduct\_mapping\_lawson\_bods             |                                                                                                                           |
| sku\_barproduct\_mapping\_p10\_bods                |                                                                                                                           |
| sku\_brand\_mapping\_masterdata                    |                                                                                                                           |
| sku\_gpp\_mapping                                  |                                                                                                                           |
| sku\_gpp\_mapping\_sgm                             |                                                                                                                           |
| soldto\_barcust\_mapping                           |                                                                                                                           |
| soldto\_shipto\_barcust\_mapping                   |                                                                                                                           |
| umm\_closedate\_config                             |                                                                                                                           |
| volume\_conv\_sku                                  |                                                                                                                           |
