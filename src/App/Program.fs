
open GraphBLAS.FSharp.Backend
open Brahma.FSharp
open System.IO
open GraphBLAS.FSharp.Backend.Common

type System.Random with
    /// Generates an infinite sequence of random numbers within the given range.
        member this.GetValues(minValue, maxValue) =
            Seq.initInfinite (fun _ -> this.Next(minValue, maxValue))

[<EntryPoint>]
let main argv =
    let clContext = ClContext(ClDevice.GetFirstAppropriateDevice())
    let processor = clContext.QueueProvider.CreateQueue()

    let numOfSectors = 10
    // let n = 1000 * numOfSectors
    let n = 10 * numOfSectors
    let workGroupSize = 128
    let path = "test.mtx"

    let r = System.Random()
    let nums = r.GetValues(1, 20) |> Seq.take 4 |> Seq.toArray

    let maxValue = 70

    let rsort = RadixSort.runInplace clContext workGroupSize

    let mutable res = true

    for k in 0 .. 10 do
        printfn "%A ..." k
        let prop = r.GetValues(0, maxValue) |> Seq.take n |> Seq.toArray

        printfn "%A" prop

        let n = prop.Length
        let keys = clContext.CreateClArray(
            prop
            // [|0..n|]
            // |> Array.map (fun i -> uint64 (n - i))
        )
        let vals = clContext.CreateClArray(prop)
        let mutable nums = [||]//[| for i in 0 .. numOfSectors - 2 do n / numOfSectors |]
        if numOfSectors > 1 then
            nums <- r.GetValues(1, n / (numOfSectors - 1)) |> Seq.take (numOfSectors - 1) |> Seq.toArray

        let mutable sum = 0
        let mutable con = [||]
        for i in 0 .. numOfSectors - 2 do
            con <-
                [|0..nums.[i]-1|]
                |> Array.map (fun k -> if k = nums.[i] - 1 then 1 else 0)
                |> Array.append con
            sum <- sum + nums.[i]
        con <-
            [|0..prop.Length-sum-1|]
            |> Array.map (fun k -> if k = prop.Length-sum - 1 then 1 else 0)
            |> Array.append con

        let sectorTails = clContext.CreateClArray(
            con
            // [|0..prop.Length-1|]
            // |> Array.map (fun i ->  i / (prop.Length - 1))
        )

        let keys, vals = rsort processor keys vals sectorTails maxValue

        let arr = Array.zeroCreate keys.Length
        let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(keys, arr, ch))
        printfn "%A" arr

        let mutable prop2 = [||]
        let len = n / numOfSectors
        let mutable acc = 0
        for j in 0 .. numOfSectors - 2 do
            let prop3 = Array.sort prop.[acc .. acc + nums.[j] - 1]
            acc <- acc + nums.[j]
            prop2 <- Array.append prop2 prop3
        let prop3 = Array.sort prop.[acc .. prop.Length - 1]
        prop2 <- Array.append prop2 prop3
        printfn "%A" prop2

        let resCurr =
            (arr, prop2)
            ||> Array.zip
            |> Array.map (fun (a, b) -> a = b)
            |> Array.fold (&&) true
        printfn "%A" resCurr
        res <- res && resCurr

    printfn "Итого %A" res

    0
