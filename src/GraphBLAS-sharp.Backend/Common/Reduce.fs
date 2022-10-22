namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp
open Microsoft.FSharp.Control
open Microsoft.FSharp.Quotations

module Reduce =
    let run
        (clContext: ClContext)
        (workGroupSize: int)
        (opAdd: Expr<'a -> 'a -> 'a>)
        (zero: 'a)
        =

        let reduce =
            <@
                fun (ndRange: Range1D) length (inputArray: ClArray<'a>) (totalSum: ClCell<'a>) ->

                    let gid = ndRange.GlobalID0
                    let lid = ndRange.LocalID0

                    let i = (gid - lid) * 2 + lid

                    let localValues = localArray<'a> workGroupSize

                    if i + workGroupSize < length then
                        localValues[lid] <- (%opAdd) inputArray[i] inputArray[i + workGroupSize]
                    elif i < length then
                        localValues[lid] <- inputArray[i]
                    else
                        localValues[lid] <- zero
                    barrierLocal ()

                    let mutable step = 2

                    while step <= workGroupSize do
                        if lid < workGroupSize / step then
                            let firstValue = localValues[lid]
                            let secondValue = localValues[lid + workGroupSize / step]

                            localValues[lid] <- (%opAdd) firstValue secondValue

                        step <- step <<< 1

                        barrierLocal ()

                    if lid = 0 then
                       atomic (%opAdd) totalSum.Value localValues[0] |> ignore
            @>

        let kernel = clContext.Compile(reduce)

        fun (processor: MailboxProcessor<_>) (valuesArray: ClArray<'a>) ->

            let ndRange = Range1D.CreateValid(valuesArray.Length, workGroupSize)

            let totalSum =
                clContext.CreateClCell(zero)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                            valuesArray.Length
                            valuesArray
                            totalSum)
                )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            totalSum
