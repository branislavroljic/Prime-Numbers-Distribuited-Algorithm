# Prime-Numbers-Distributed-Algorithm

* A list of ranges of positive integers of arbitrary size (i.e. without a number limit) is fed to the algorithm input
digit).

* The output of the algorithm is the total number of prime numbers that exist in the passed ranges.

* The work is distributed to more physical machines, so that the use of all machines is maximized. Balancing has been performed
work so that each physical machine processes an equal number of numbers from the range.
  * In cases where it is necessary to process ranges of significantly different widths, the ranges are already segmented into smaller and distributed on more machines
  * Recursive range division is enabled, depending on the number of machines available.
 
* Algorithm parameterization is enabled.
  * The parameter specifies the maximum number of machines on which tasks are calculated.
  * The parameter specifies the maximum branching depth when dividing the range. 
