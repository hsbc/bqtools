1.0.3 December 5th 2022
- Update and move to hsbc repo and update CHANGELOG to be accurate update LICENSE from MIT to Apache 2

1.0.2 April 20th 2022
- Remove google-cloud dependency and bump logging dependency to v3 so we get "extrs" support for log entries

1.0.1 April 20th 2022
- Move deepdiff dependency as only required in testing 

1.0.0 Feb 25th 2022
- Added support in generated head view of _PARTITIONTIME is null so included latest streamed data

0.4.121 January 18th 2022
- Removed use of use_2_to_3 which breaks ins etup tools > v58 so moved to Python 3 only

0.4.12 October 3rd 2021

- Resolved issues if source was of type TABLE and destination is type VIEW or MATERIALIZED_VIEW instead of failing now replaces the target object with a table.