//read documents from disk 
//we tokenize and stem each document
//we collect term, doc_id, position into tuples
// build an inverted index
// which is just term -> [ Posting{doc_id, frequency, [positions]}, ...]
write to disk four files
 - Postings with delta encoded doc id and vbyte compression
 - offset - bit offset for each terms posting in the posting file 
 - Alphas sorted vocabularywith collection frequencies 
 - Docs document paths and lengths
 - Trigram index would be built and kept in memory for spelling corrections