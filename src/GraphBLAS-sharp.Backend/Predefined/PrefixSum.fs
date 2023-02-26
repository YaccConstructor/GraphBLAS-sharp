namespace GraphBLAS.FSharp.Backend.Predefined

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
module internal PrefixSum =
    let standardExcludeInplace (clContext: ClContext) workGroupSize =

        let scan =
            ClArray.prefixSumExcludeInplace <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) (totalSum: ClCell<int>) ->

            scan processor inputArray totalSum 0

    let standardIncludeInplace (clContext: ClContext) workGroupSize =

        let scan =
            ClArray.prefixSumIncludeInplace <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) (totalSum: ClCell<int>) ->

            scan processor inputArray totalSum 0

    let standardInclude (clContext: ClContext) workGroupSize =

        let scan =
            ClArray.prefixSumInclude <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (inputArray: ClArray<int>) (totalSum: ClCell<int>) ->

            scan processor allocationMode inputArray totalSum 0

    let standardExclude (clContext: ClContext) workGroupSize =

        let scan =
            ClArray.prefixSumExclude <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (inputArray: ClArray<int>) (totalSum: ClCell<int>) ->

            scan processor allocationMode inputArray totalSum 0

    let standardExcludeInplaceLengthToHost clContext workGroupSize =

        let scan = standardExcludeInplace clContext workGroupSize

        let resultLength = Array.zeroCreate<int> 1

        fun (processor: MailboxProcessor<_>) (positions: ClArray<int>) ->

            let resultLengthGpu = clContext.CreateClCell 0

            let _, r =
                scan processor positions resultLengthGpu

            let res =
                processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultLength, ch))

            processor.Post(Msg.CreateFreeMsg<_>(r))

            res.[0]


