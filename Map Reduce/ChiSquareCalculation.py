
import itertools
from collections import defaultdict
import json
import re
from mrjob.job import MRJob
from mrjob.step import MRStep

class ChiSquareCalculation(MRJob):


    def __init__(self, *args, **kwargs):
        super(ChiSquareCalculation, self).__init__(*args, **kwargs)

        self.category_totals = {}


    def mapper(self, _, line):
        data = json.loads(line)
        text = data.get('reviewText', '')
        category = data.get('category', '')

        delimiters = r'[\s\d()\[\]\{\}\.!?,;:+=\-_"\'`~#@&*%€$§\\/]+'
        words = re.split(delimiters, text.lower())

        stopwords = set()
        with open("/home/soroush/Desktop/Files/Kurses/Data_Intensive_Computing/github/Data-Intenseive-Computing/Map Reduce/stopwords.txt", "r", encoding="utf-8") as f:
            stopwords.update(line.strip() for line in f)

        words = [word for word in words if len(word) > 1 and word not in stopwords]

        for term in words:
          yield ("TERM_CATEGORY", term, category), 1

    # Emit category counts (C)
        yield ("CATEGORY_TOTAL", category), 1

    # Emit term counts (B)
        for term in set(words):  # Count each term once per document
           yield ("TERM_TOTAL", term), 1



    def reducer_aggregate(self, key, counts):
      key_type, *rest = key

      if key_type == "TERM_CATEGORY":
        term, category = rest
        yield ("TERM_CATEGORY", term, category), sum(counts)
      elif key_type == "CATEGORY_TOTAL":
        category = rest[0]
        yield ("CATEGORY_TOTAL", category), sum(counts)
      elif key_type == "TERM_TOTAL":
        term = rest[0]
        yield ("TERM_TOTAL", term), sum(counts)


    def reducer_join_data(self, key, values):
        key_type, *rest = key
        count = sum(values)

        if key_type == "TERM_CATEGORY":
            term, category = rest
            # Emit for joining (term is the join key)
            yield term, ("TERM_CATEGORY", category, count)
        elif key_type == "TERM_TOTAL":
            term = rest[0]
            yield term, ("TERM_TOTAL", count)
        elif key_type == "CATEGORY_TOTAL":
            category = rest[0]
            yield  "CATEGORY_TOTAL", (category, count)  # Global counts


    def final_reducer(self, key, values):
        if key == "CATEGORY_TOTAL":
            category_totals = {}
            for val in values:
                category, count = val
                category_totals[category] = count
            yield "Final" , ("Category" , category_totals)

        else:
          term_total = 0
          term_category_counts = {}
          for value in values:
            value_type, *rest = value

            if value_type == "TERM_TOTAL":
                term_total = rest[0]
            elif value_type == "TERM_CATEGORY":
                category, count = rest
                term_category_counts[category] = count
            yield "Final" , ( "Terms" ,key ,term_total, term_category_counts)



    def reducer_calculate_chi(self, term, values):
        category_totals = {}
        values = list(values)
        Dictionary = []
        for val in values:
          if val[0] == "Category":
            category_totals = val[1]

        for val in values:
          if val[0] == "Terms":
            term_total = val[2]
            term_category_counts = val[3]
            # Now compute Chi-Square for each (term, category)
            for category, A in term_category_counts.items():
                B = term_total - A
                C = category_totals.get(category, 0) - A
                D = sum(category_totals.values()) - (A + B + C)
                N = A + B + C + D
                if (A + B) * (A + C) * (B + D) * (C + D) != 0:
                  chi_square = (N * (A * D - B * C) ** 2) / ((A + B) * (A + C) * (B + D) * (C + D))
                  Dictionary.append(val[1])
                
                  yield ( category) ,   (val[1],chi_square)
        Dictionary = set( Dictionary)
        Dictionary = list(Dictionary)
        sorted_dictionary = sorted(Dictionary, key=str.lower)
        formatted_Dictionary = ', '.join([f"{item}" for item in sorted_dictionary])

        yield "Dictionary: " , formatted_Dictionary





    def ordering(self , key , values):
        values = list(values)
        data = []
        seen = set()
        for item in values:
          word = (item[0], item[1])
          if word not in seen:
              seen.add(word)
              data.append(item)
        sorted_data = sorted(data, key=lambda x: x[1])
        final_data = sorted_data[-75:]
        formatted_value = ', '.join([f"{item[0]}:{item[1]}" for item in final_data])
        yield key , formatted_value
        
        if key == "Dictionary: ":
         
          yield key , values


    def steps(self):
        return [
            MRStep(mapper = self.mapper ,reducer= self.reducer_aggregate ),
            MRStep(reducer=self.reducer_join_data),
            MRStep(reducer=self.final_reducer),
            MRStep(reducer=self.reducer_calculate_chi),
            MRStep(reducer=self.ordering)

        ]

if __name__ == '__main__':
    ChiSquareCalculation.run()
