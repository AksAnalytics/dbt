# UMM NA Migration Checklist for Views and Stored Procedures in `DW` schema

## Views

| Object                             | Description                                                                                                                         |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| vw\_dim\_customer                  | Dimension view that combines the dim\_customer and dim\_customer\_restatement tables together for customer restatement requirements |
| vw\_dim\_processing\_rule          |                                                                                                                                     |
| vw\_dim\_product                   | Dimension view that combines the dim\_product and dim\_product\_restatement tables together for product restatement requirements    |
| vw\_fact\_pnl\_commercial\_orig    | Fact view that limits the data from the fact\_pnl\_commercial\_orig table to dates before the last week's closing date              |
| vw\_fact\_pnl\_commercial\_stacked | Fact view that limits the data from the fact\_pnl\_commercial\_stacked table to dates before the last week's closing date           |
| vw\_fact\_pnl\_ocos\_stacked       | Fact view that limits the data from the fact\_pnl\_ocos\_stacked table to dates before the last week's closing date                 |

## Stored Procedures

| Object                                                     | Description                                                                                           |
| ---------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| p\_build\_dim\_business\_unit                              | Loads the dw.dim\_business\_unit dimension table                                                      |
| p\_build\_dim\_currency                                    | Loads the dw.dim\_currency dimension table                                                            |
| p\_build\_dim\_customer                                    | Loads the dw.dim\_customer dimension table                                                            |
| p\_build\_dim\_customer\_restatement                       |                                                                                                       |
| p\_build\_dim\_dataprocessing\_outcome                     | Loads the dw.dim\_dataprocessing\_outcome dimension table                                             |
| p\_build\_dim\_dataprocessing\_rule                        | Loads the dw.dim\_dataprocessing\_rule dimension table                                                |
| p\_build\_dim\_date                                        | Loads the dw.dim\_date dimension table                                                                |
| p\_build\_dim\_product                                     | Loads the dw.dim\_product dimension table                                                             |
| p\_build\_dim\_product\_restatement                        |                                                                                                       |
| p\_build\_dim\_scenario                                    | Loads the dw.dim\_scenario dimension table                                                            |
| p\_build\_dim\_source\_system                              | Loads the dw.dim\_source\_system dimension table                                                      |
| p\_build\_dim\_transactional\_attributes                   | Loads the dw.dim\_transactional\_attributes dimension table                                           |
| p\_build\_fact\_pnl\_commercial\_allocation\_rule\_09      |                                                                                                       |
| p\_build\_fact\_pnl\_commercial\_allocation\_rule\_13      |                                                                                                       |
| p\_build\_fact\_pnl\_commercial\_allocation\_rule\_21      |                                                                                                       |
| p\_build\_fact\_pnl\_commercial\_allocation\_rule\_22      |                                                                                                       |
| p\_build\_fact\_pnl\_commercial\_allocation\_rule\_23      |                                                                                                       |
| p\_build\_fact\_pnl\_commercial\_allocation\_rule\_26      |                                                                                                       |
| p\_build\_fact\_pnl\_commercial\_allocation\_rule\_27      |                                                                                                       |
| p\_build\_fact\_pnl\_commercial\_allocation\_rule\_28      |                                                                                                       |
| p\_build\_fact\_pnl\_commercial\_not\_allocated            |                                                                                                       |
| p\_build\_fact\_pnl\_commercial\_orig                      |                                                                                                       |
| p\_build\_fact\_pnl\_commercial\_stacked                   | Loads the dw.fact\_pnl\_commercial\_stacked fact table (from the dw.fact\_pnl\_commercial fact table) |
| p\_build\_fact\_pnl\_ocos\_allocation\_rule\_100           |                                                                                                       |
| p\_build\_fact\_pnl\_ocos\_allocation\_rule\_101           |                                                                                                       |
| p\_build\_fact\_pnl\_ocos\_allocation\_rule\_102\_104\_gap |                                                                                                       |
| p\_build\_fact\_pnl\_ocos\_allocation\_rule\_105           |                                                                                                       |
| p\_build\_fact\_pnl\_ocos\_stacked                         | Loads the dw.fact\_pnl\_ocos\_stacked fact table (from the dw.fact\_pnl\_ocos fact table)             |
| run\_umm\_agm\_anton                                       | \*\* Used for testing; should be deprecated \*\*                                                      |
| run\_umm\_commercial                                       | \*\* Used for testing; should be deprecated \*\*                                                      |
| run\_umm\_sgm\_agm                                         | \*\* Used for testing; should be deprecated \*\*                                                      |