OPTIMISATIONS DONE:
  1) FOR THE BULK INSERTION:
      We have implemented bulk insertion by not just inserting record by record.
      The csv of records are taken and once block size is reached after reading each row,
      we insert the whole block in the table at the last pages. So that nothing else is changed 
      in the table. And also, decreadsng buffer fetches.
