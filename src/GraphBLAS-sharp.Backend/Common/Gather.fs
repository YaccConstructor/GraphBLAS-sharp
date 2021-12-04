namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp.OpenCL

module internal Gather =
    let run
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (positions: ClArray<int>)
        (inputArray: ClArray<'a>) =

        let outputArray =
            clContext.CreateClArray(
                positions.Length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let size = outputArray.Length

        let gather =
            <@
                fun (ndRange: Range1D)
                    (positions: ClArray<int>)
                    (inputArray: ClArray<'a>)
                    (outputArray: ClArray<'a>) ->

                    let i = ndRange.GlobalID0

                    if i < size then outputArray.[i] <- inputArray.[positions.[i]]
            @>

        let kernel = clContext.CreateClKernel(gather)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        positions
                        inputArray
                        outputArray)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

        outputArray
