namespace GraphBLAS.FSharp.Backend.Predefined

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common

module internal PrefixSum =
    let standardExcludeInplace (clContext: ClContext) workGroupSize =

        let scan =
            ClArray.prefixSumExcludeInplace <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) ->

            scan processor inputArray 0

    let standardIncludeInplace (clContext: ClContext) workGroupSize =

        let scan =
            ClArray.prefixSumIncludeInplace <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) ->

            scan processor inputArray 0

    let standardInclude (clContext: ClContext) workGroupSize =

        let scan =
            ClArray.prefixSumInclude <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (inputArray: ClArray<int>) ->

            scan processor allocationMode inputArray 0

    let standardExclude (clContext: ClContext) workGroupSize =

        let scan =
            ClArray.prefixSumExclude <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (inputArray: ClArray<int>) ->

            scan processor allocationMode inputArray 0
