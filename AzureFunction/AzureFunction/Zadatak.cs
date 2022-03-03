using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace PrimesWithAzure {

    public class Range {
        public ulong start;
        public ulong end;

        public Range(ulong start, ulong end) {
            this.start = start;
            this.end = end;
        }

        public override bool Equals(object obj) {
            return obj is Range range &&
                   start == range.start &&
                   end == range.end;
        }

        public override int GetHashCode() {
            return HashCode.Combine(start, end);
        }

        public override string ToString() {
            return "start: " + start + " end: " + end + " ";
        }

    }

    public class RangeOfPrimes {
        public Range range;
        public List<ulong> primes;

        public RangeOfPrimes(Range range, List<ulong> primes) {
            this.range = range;
            this.primes = primes;
        }
        public override string ToString() {
            return "number of primes: " + primes.Count + primes.ToString();
        }
    }


    public static class Zadatak {

        //limit za rekurzivnu podjelu
        public static int LIMIT = 500;

        //broj masina i maksimalnu dubinu specifikuje onaj ko šalje zahtjev
        public static int numberOfMachines = -1;
        public static int maxDepth;

        [FunctionName("OrchestratePrimes")]
        public static async Task<JArray> RunOrchestrator([OrchestrationTrigger] IDurableOrchestrationContext context) {

            dynamic requestBody = context.GetInput<string>();
            dynamic request = JsonConvert.DeserializeObject(requestBody);

            numberOfMachines = request.numberOfMachines;
            maxDepth = request.maxDepth ?? 0;

            List<Task<List<RangeOfPrimes>>> tasks = new List<Task<List<RangeOfPrimes>>>();

            List<Range> rangesList = new List<Range>();

            //citanje opsega
            foreach (var range in request.ranges) {
                
                if (range.start > range.end)
                    continue;
                rangesList.Add(new Range((ulong)range.start, (ulong)range.end));
            }

            //podjela posla na masine
            List<List<(Range, (ulong start, ulong end))>> listForCurrentMachine = ScheduleRangesOnMachines(numberOfMachines, rangesList);

            //pokretanje masina
            for (int machineCounter = 0; machineCounter < numberOfMachines; machineCounter++) {
                tasks.Add(context.CallActivityAsync<List<RangeOfPrimes>>("FindPrimesInRange", listForCurrentMachine[machineCounter]));
            }

            //zavrsetak posla
            List<RangeOfPrimes>[] result = await Task.WhenAll(tasks);

            //grupisanje listi prostih brojeva po opsegu kojem odgovaraju
            var groups = result.SelectMany(el => el).GroupBy(el => el.range).ToDictionary(g => g.Key, g => g.Select(x => x.primes));

            //formatiranje izlaza
            JArray formattedOutput = new JArray();
            List<ulong> mergedList = new List<ulong>();
            foreach (var group in groups) {
                mergedList.Clear();
                foreach (var list in group.Value) {
                    mergedList.AddRange(list);
                }

                formattedOutput.Add(JObject.FromObject(new {
                    range = new {
                        start = group.Key.start,
                        end = group.Key.end,
                        numberOfPrimes = mergedList.Count,
                        primes = mergedList
                    }
                })) ;
                
            }

            return formattedOutput;
        }

        //podjela posla na masine
        private static List<List<(Range, (ulong start, ulong end))>> ScheduleRangesOnMachines(int numberOfMachines, List<Range> rangesList) {

            List<List<(Range, (ulong start, ulong end))>> jobListForMachines = new List<List<(Range, (ulong start, ulong end))>>(numberOfMachines);

            //prosjecan broj elemenata koji treba da obradi svaka masina
            ulong averageNumberOfNumbersToCheck = (ulong)Math.Round(rangesList.Select(r => r.end - r.start).Aggregate((a,b) => a + b) / (double)numberOfMachines);

            ulong startPos = 0; int rangeCounter = 0, machineCounter;
            for (machineCounter = 0; machineCounter < numberOfMachines - 1; machineCounter++) {

                jobListForMachines.Add(new List<(Range, (ulong start, ulong end))>());
                //broj brojeva koji masina treba obraditi
                ulong currentNumberOfNumbersToCheck = 0;
                while (currentNumberOfNumbersToCheck < averageNumberOfNumbersToCheck) {
                    
                    ulong currentStartNumber = startPos == 0 ? rangesList[rangeCounter].start : startPos;

                    // moze uzeti sve elemente do kraja trenutnog opsega
                    if (currentNumberOfNumbersToCheck + rangesList[rangeCounter].end - currentStartNumber < averageNumberOfNumbersToCheck) {
                        currentNumberOfNumbersToCheck += (rangesList[rangeCounter].end - currentStartNumber);
                        jobListForMachines[machineCounter].Add((rangesList[rangeCounter], (currentStartNumber, rangesList[rangeCounter].end)));
                        //predji na sljedeci opseg u listi
                        rangeCounter++;
                        startPos = rangesList[rangeCounter].start;
                    }
                    else {
                        //izracunaj koliko elemenata moze uzeti iz opsega
                        ulong max = averageNumberOfNumbersToCheck - currentNumberOfNumbersToCheck;
                        //dodaj elemente
                        currentNumberOfNumbersToCheck += max;
                        jobListForMachines[machineCounter].Add((rangesList[rangeCounter], (currentStartNumber, currentStartNumber + max)));
                        //startna pozicija je zadnji uzeti broj iz opsega + 1
                        startPos = currentStartNumber + max;

                    }
                }
            }
            //posljednja masina uzima ostatak trenutnog opsega
            jobListForMachines.Add(new List<(Range, (ulong start, ulong end))>());
            jobListForMachines[machineCounter].Add((rangesList[rangeCounter], (startPos, rangesList[rangeCounter].end)));
            rangeCounter++;
            //i sve preostale neobradjene opsege
            while (rangeCounter < rangesList.Count) {
                jobListForMachines[machineCounter].Add((rangesList[rangeCounter], (rangesList[rangeCounter].start, rangesList[rangeCounter].end)));
                rangeCounter++;
            }

            return jobListForMachines;
        }

        [FunctionName("FindPrimesInRange")]
        public static List<RangeOfPrimes> FindPrimesInRange([ActivityTrigger] List<(Range range, (ulong start, ulong end))> list) {
            List<RangeOfPrimes> primesList = new List<RangeOfPrimes>();

            //racunanje prostih brojeva za svaki podopseg
            foreach (var item in list) {
                primesList.Add(new RangeOfPrimes(item.range, PrimesInRange(item.Item2.start, item.Item2.end, maxDepth)));
            }
            return primesList;
        }



        [FunctionName("Start")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
             [DurableClient] IDurableOrchestrationClient starter,
            ILogger log) {
            log.LogInformation("C# HTTP trigger function processed a request.");


            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            string instanceId = await starter.StartNewAsync("OrchestratePrimes", input: requestBody);

            return starter.CreateCheckStatusResponse(req, instanceId);
        }

        //pronadji proste brojeve u opsegu
        public static List<ulong> PrimesInRange(ulong L, ulong R, int depth) {
            int flag;
            ulong range = R - L;

            List<ulong> primes = new List<ulong>();
            List<ulong> primesFirstHalf = null;
            List<ulong> primesSecondHalf = null;

            //podijeli rekurzivno posao na vise niti iste masine dok god broj elemenata prelazi LIMIT
            if (range > (ulong)LIMIT && depth > 0) {
                Parallel.Invoke(
                     () => primesFirstHalf = PrimesInRange(L, L + range / 2, depth - 1),
                     () => primesSecondHalf = PrimesInRange(L + range / 2, R, depth - 1)
                );
                //spojim rezultat
                primes.AddRange(primesFirstHalf);
                primes.AddRange(primesSecondHalf);
            }
            else {
                for (ulong i = L; i <= R; i++) {

                    if (i == 1 || i == 0)
                        continue;

                    //da li je broj prost
                    flag = 1;

                    //provjera 
                    for (ulong j = 2; j <= i / 2; ++j) {
                        if (i % j == 0) {
                            flag = 0;
                            break;
                        }
                    }

                    // flag = 1 prost
                    //  flag = 0 nije
                    if (flag == 1) {
                        primes.Add(i);
                    }

                }
            }
            return primes;
        }

    }
}
