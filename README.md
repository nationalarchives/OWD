# OWD
Operation War Diary raw data processing

# Summary
The code will eventually include perlpod documentation and comments so I'll summarise the raw dataset here.

The data contributed by Operation War Diary volunteers is provided to us by Zooniverse as a MongoDB database consisting of four collections:
* war\_diary\_groups (where each document is a National Archives Catalogue Item - a war diary, or part of a diary where the diary was too large to provide as a single download. Each group has a zooniverse_id field which serves as the foreign key to the pages/subjects collection)
* war\_diary\_subjects (where each document is a page from a diary. Each subject also has a zooniverse\_id field to act as a foreign key for the classifications collection. The groups.zooniverse\_id field references the group (diary) that the page belongs to)
* war\_diary\_classifications (where each document represents the collection of classifications made by a single user. Each completed page therefore has between 5 and 7 classifications)
* war\_diary\_users (where each document is a volunteer contributor)
