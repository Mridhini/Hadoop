from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env

class Proj1(MRJob):
    
    def mapper_init(self):
        self.tmp = {}
        self.tau = 0

    def mapper(self, _, line):
        parts = line.split("\t")

        if len(parts) == 7:
            conti = parts[0]
            country = parts[1]
            city = parts[2]
            day = int(parts[3])
            month = int(parts[4])
            year = int(parts[5])
            temp_fahrenheit = float(parts[6])

            temp_celsius = (temp_fahrenheit - 32) * 5.0 / 9.0

            city_year = f"{city},{year}"
            yield (city + ",9999"), (temp_celsius, 1)
            yield (city_year), (temp_celsius, 1)
            
    def combiner(self, key, values):
        sum_temp, counter = 0, 0
        for temp, c in values:
            sum_temp += temp
            counter += c
        if counter:
            city, year = key.split(",", 1)
            yield city, (sum_temp, counter, year)

    def reducer_init(self):
        self.city_averages = {}
        tau_str = jobconf_from_env('myjob.settings.tau')
        try:
            self.tau = float(tau_str) if tau_str is not None else 0.0
        except ValueError:
            self.tau = 0.0

    def reducer(self, key, values):
        city = key
        total_sum, total_counter = 0, 0
        years_data = {}

        for sum_temp, counter, year in values:
            total_sum += sum_temp
            total_counter += counter
            if year not in years_data:
                years_data[year] = {'sum': sum_temp, 'counter': counter}
            else:
                years_data[year]['sum'] += sum_temp
                years_data[year]['counter'] += counter

        if total_counter:
            overall_avg = total_sum / total_counter
            if '9999' in years_data:
                self.city_averages[city] = years_data['9999']['sum'] / years_data['9999']['counter']

            for year, data in sorted(years_data.items(), key=lambda x: int(x[0]), reverse=True):
                if year != "9999":
                    avg_temp = data['sum'] / data['counter']
                    if city in self.city_averages:
                        difference = avg_temp - self.city_averages[city]
                        if difference > self.tau:
                            yield city, f"{year},{difference}"

    SORT_VALUES = True

    def steps(self):

        JOBCONF = {
            'stream.num.map.output.key.fields': 2,
            'mapreduce.map.output.key.field.separator': ',',
            'mapreduce.partition.keypartitioner.options': '-k1,1',
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options': '-k1,1 -k2,2nr'
        }
        return [
            MRStep(
                jobconf=JOBCONF,
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                combiner=self.combiner,
                reducer_init=self.reducer_init,
                reducer=self.reducer
            )
        ]

if __name__ == '__main__':
    Proj1.run()

