from whoosh.index import create_in
from whoosh import scoring
from whoosh.fields import *

import argparse

import logging
import sys

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

log = logging.getLogger("EHR-QC")
log.setLevel(logging.INFO)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
log.addHandler(ch)

from ehrqc.standardise import Config


def fetchMatchingConceptFuzzy(searchPhrase, standardConcepts):
    matchingConcept = process.extract(searchPhrase, standardConcepts, limit=1, scorer=fuzz.token_sort_ratio)
    return matchingConcept[0][0]


def fetchMatchingConceptMedcatFromCat(searchPhrase, cat):
    entities = cat.get_entities(searchPhrase)['entities']
    matchingConceptPrettyName = None
    maxContextSimilarityScore = 0
    matchingConceptId = None
    for key in entities.keys():
        entity = entities[key]
        contextSimilarityScore = float(entity['context_similarity'])
        if contextSimilarityScore > maxContextSimilarityScore:
            matchingConceptPrettyName = entity['pretty_name']
            maxContextSimilarityScore = contextSimilarityScore
            matchingConceptId = entity['cui']
    return matchingConceptPrettyName, matchingConceptId


def fetchMatchingConceptFromReverseIndex(searchPhrase, ix):
        from whoosh import qparser
        matchingConcept = None
        with ix.searcher() as searcher:
            andParser = qparser.QueryParser("concept", ix.schema)
            andParser.add_plugin(qparser.FuzzyTermPlugin())
            andSearchTerm = "~1 AND ".join(str(searchPhrase).split()) + "~1"
            andQueryFuzzy = andParser.parse(andSearchTerm)
            andResultsFuzzy = searcher.search(andQueryFuzzy)
            orParser = qparser.QueryParser("concept", ix.schema)
            orParser.add_plugin(qparser.FuzzyTermPlugin())
            orSearchTerm = " OR ".join(str(searchPhrase).split())
            orQueryFuzzy = orParser.parse(orSearchTerm)
            orResultsFuzzy = searcher.search(orQueryFuzzy)
            parser = qparser.QueryParser("concept", ix.schema)
            query = parser.parse(str(searchPhrase))
            results = searcher.search(query)
            results.extend(andResultsFuzzy)
            results.extend(orResultsFuzzy)
            if len(results) > 0:
                matchingConcept = results[0]['concept']
        return matchingConcept


def fetchMatchingConceptFromMajorityVotingPlus(searchPhrase, standardConcepts, ix, cat):
    medcatConceptName, medcatConceptId = fetchMatchingConceptMedcatFromCat(searchPhrase=searchPhrase, cat=cat)
    fuzzyConceptName = fetchMatchingConceptFuzzy(searchPhrase=searchPhrase, standardConcepts=standardConcepts.concept_name)
    fuzzyConceptId = None
    if not standardConcepts[standardConcepts.concept_name == fuzzyConceptName].concept_code.empty:
        fuzzyConceptId = standardConcepts[standardConcepts.concept_name == fuzzyConceptName].concept_code.values[0]
    reverseIndexConceptName = fetchMatchingConceptFromReverseIndex(searchPhrase=searchPhrase, ix=ix)
    reverseIndexConceptId = None
    if not standardConcepts[standardConcepts.concept_name == reverseIndexConceptName].concept_code.empty:
        reverseIndexConceptId = standardConcepts[standardConcepts.concept_name == reverseIndexConceptName].concept_code.values[0]
    if (medcatConceptName and fuzzyConceptName and reverseIndexConceptName and (medcatConceptName == fuzzyConceptName == reverseIndexConceptName)):
        return [(searchPhrase, medcatConceptName, medcatConceptId, fuzzyConceptName, fuzzyConceptId, reverseIndexConceptName, reverseIndexConceptId, medcatConceptName, medcatConceptId, 'Medcat + Fuzzy + RevIndex')]
    elif (medcatConceptName and fuzzyConceptName and (medcatConceptName == fuzzyConceptName != reverseIndexConceptName)):
        return [(searchPhrase, medcatConceptName, medcatConceptId, fuzzyConceptName, fuzzyConceptId, reverseIndexConceptName, reverseIndexConceptId, medcatConceptName, medcatConceptId, 'Medcat + Fuzzy')]
    elif (medcatConceptName and reverseIndexConceptName and (medcatConceptName == reverseIndexConceptName != fuzzyConceptName)):
        return [(searchPhrase, medcatConceptName, medcatConceptId, fuzzyConceptName, fuzzyConceptId, reverseIndexConceptName, reverseIndexConceptId, medcatConceptName, medcatConceptId, 'Medcat + RevIndex')]
    elif (reverseIndexConceptName and fuzzyConceptName and (reverseIndexConceptName == fuzzyConceptName != medcatConceptName)):
        return [(searchPhrase, medcatConceptName, medcatConceptId, fuzzyConceptName, fuzzyConceptId, reverseIndexConceptName, reverseIndexConceptId, reverseIndexConceptName, fuzzyConceptId, 'Fuzzy + RevIndex')]
    else:
        if medcatConceptName:
            return [(searchPhrase, medcatConceptName, medcatConceptId, fuzzyConceptName, fuzzyConceptId, reverseIndexConceptName, reverseIndexConceptId, medcatConceptName, medcatConceptId, 'Medcat')]
        if fuzzyConceptName:
            return [(searchPhrase, medcatConceptName, medcatConceptId, fuzzyConceptName, fuzzyConceptId, reverseIndexConceptName, reverseIndexConceptId, fuzzyConceptName, fuzzyConceptId, 'Fuzzy')]
        if reverseIndexConceptName:
            return [(searchPhrase, medcatConceptName, medcatConceptId, fuzzyConceptName, fuzzyConceptId, reverseIndexConceptName, reverseIndexConceptId, reverseIndexConceptName, reverseIndexConceptId, 'RevIndex')]


def generateCustomMappingsForReview(domainId, vocabularyId, conceptClassId, model_pack_path, conceptsPath, conceptNameRow, mappedConceptSavePath):

    from ehrqc.standardise.Utils import getConnection
    import pandas as pd

    log.info('Getting connection')

    con = getConnection()
    standardConceptsQuery = """
    select
    *
    from
    """ + Config.lookup_schema_name + """.concept
    where
    domain_id = '""" + domainId + """'
    and vocabulary_id = '""" + vocabularyId + """'
    and concept_class_id = '""" + conceptClassId + """'
    """

    log.info('Reading concepts')

    standardConceptsDf = pd.read_sql_query(standardConceptsQuery, con)

    log.info('Creating reverse index')

    schema = Schema(concept=TEXT(stored=True, analyzer=analysis.StemmingAnalyzer()))

    import os

    if not os.path.isdir("/tmp/indexdir"):
        os.makedirs("/tmp/indexdir")

    ix = create_in("/tmp/indexdir", schema)

    writer = ix.writer()
    for standardConcept in standardConceptsDf.concept_name:
        writer.add_document(concept=standardConcept)
    writer.commit()

    log.info('Initializing Medcat')

    from medcat.cat import CAT
    cat = CAT.load_model_pack(model_pack_path)

    conceptsDf = pd.read_csv(conceptsPath)

    outRows = []

    from tqdm import tqdm

    for i, row in tqdm(conceptsDf.iterrows(), total=conceptsDf.shape[0]):

        if(pd.isna(row[conceptNameRow])):
            continue

        log.debug('Mapping concept: ' + row[conceptNameRow])

        matchingConcepts = fetchMatchingConceptFromMajorityVotingPlus(
            searchPhrase=row[conceptNameRow]
            , standardConcepts=standardConceptsDf
            , ix=ix
            , cat=cat
            )

        for matchingConcept in matchingConcepts:
            matchingConceptList = list(matchingConcept)
            outRows.append(matchingConceptList)

    matchingConceptsDf = pd.DataFrame(outRows, columns=['searchPhrase', 'medcatConceptName', 'medcatConceptId', 'fuzzyConceptName', 'fuzzyConceptId', 'reverseIndexConceptName', 'reverseIndexConceptId', 'mvpConcept', 'mvpConceptId', 'source'])
    matchingConceptsDf.to_csv(mappedConceptSavePath, index=False)


if __name__ == "__main__":

    print("Parsing command line arguments")

    parser = argparse.ArgumentParser(description='Perform concept mapping')

    parser.add_argument("domain_id", help="Domain ID of the standard vocabulary to be mapped")
    parser.add_argument("vocabulary_id", help="Vocabulary ID of the standard vocabulary to be mapped")
    parser.add_argument("concept_class_id", help="Concept class ID of the standard vocabulary to be mapped")
    parser.add_argument("concepts_path", help="Path for the concepts csv file")
    parser.add_argument("concept_name_row", help="Name of the concept name row in the concepts csv file")
    parser.add_argument("mapped_concepts_save_path", help="Path for saving the mapped concepts csv file")
    parser.add_argument("--model_pack_path", help="Path for the Medcat model_pack_path zip file")

    args = parser.parse_args()

    log.info('domain_id: ' + args.domain_id)
    log.info('vocabulary_id: ' + args.vocabulary_id)
    log.info('concept_class_id: ' + args.concept_class_id)
    log.info('concepts_path: ' + args.concepts_path)
    log.info('concept_name_row: ' + args.concept_name_row)
    log.info('mapped_concepts_save_path: ' + args.mapped_concepts_save_path)
    log.info('model_pack_path: ' + args.model_pack_path)

    generateCustomMappingsForReview(
        domainId=args.domain_id
        , vocabularyId=args.vocabulary_id
        , conceptClassId=args.concept_class_id
        , model_pack_path=args.model_pack_path
        , conceptsPath=args.concepts_path
        , conceptNameRow=args.concept_name_row
        , mappedConceptSavePath=args.mapped_concepts_save_path
        )
