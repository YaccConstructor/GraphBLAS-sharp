module Backend.SpGEMM

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Tests.Utils

open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp
open OpenCL.Net

open System
open Brahma.FSharp.OpenCL.Shared
open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Utils
open OpenCL.Net
open Backend.Common.StandardOperations

open Microsoft.FSharp.Quotations

let logger = Log.create "SpGEMM.Tests"

let defaultContext =
    let a =
        avaliableContexts ""
        |> Seq.filter
            (fun ctx ->
                let mutable e = ErrorCode.Unknown
                let device = ctx.ClContext.ClDevice.Device

                let deviceType =
                    Cl
                        .GetDeviceInfo(device, DeviceInfo.Type, &e)
                        .CastTo<DeviceType>()

                deviceType = DeviceType.Gpu)
        |> List.ofSeq
    a.[0]

let context = defaultContext.ClContext
let workGroupSize = 32

let makeTest
    context
    q
    zero
    isEqual
    plus
    mul
    spgemmFun
    (leftMatrix: 'a [,], rightMatrix: 'a [,], mask: bool [,])
    =

    let m1 =
        createMatrixFromArray2D CSR leftMatrix (isEqual zero)

    let m2 =
        createMatrixFromArray2D CSC rightMatrix (isEqual zero)

    if m1.NNZCount > 0 && m2.NNZCount > 0 then
        let expected =
            Array2D.init
            <| Array2D.length1 mask
            <| Array2D.length2 mask
            <| fun i j ->
                if mask.[i, j] then
                    (leftMatrix.[i, *], rightMatrix.[*, j])
                    ||> Array.map2 mul
                    |> Array.reduce plus
                else zero

        let expected =
            createMatrixFromArray2D COO expected (isEqual zero)

        if expected.NNZCount > 0 then
            let m1 = m1.ToBackend context
            let m2 = m2.ToBackend context
            let mask = Mask2D.FromArray2D(mask, not).ToBackend context

            let result = spgemmFun q m1 m2 mask
            let actual = Matrix.FromBackend q result

            m1.Dispose q
            m2.Dispose q
            mask.Dispose q
            result.Dispose q

            // Check result
            "Matrices should be equal"
            |> Expect.equal actual expected

let tests =

    let getCorrectnessTestName datatype =
        sprintf "Correctness on %s" datatype

    let config =
        { defaultConfig with
            //   endSize = 15
              arbitrary = [ typeof<Generators.PairOfMatricesOfCompatibleSizeWithMask> ] }

    let q = defaultContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    let logicalOr = <@ fun x y ->
        let mutable res = None

        match x, y with
        | false, false -> res <- None
        | _            -> res <- Some true

        res @>
    let logicalAnd = <@ fun x y ->
        let mutable res = None

        match x, y with
        | true, true -> res <- Some true
        | _          -> res <- None

        res @>

    [ let add = <@ fun x y ->
          let mutable res = x + y

          if res = 0 then None else (Some res) @>

      let mult = <@ fun x y -> Some (x * y) @>

      let spgemmFun = Matrix.mxm context workGroupSize add mult
      makeTest context q 0 (=) (+) (*) spgemmFun
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let add = <@ fun x y ->
          let mutable res = x + y

          if res = 0uy then None else (Some res) @>

      let mult = <@ fun x y ->
          let mutable res = x * y

          if res = 0uy then None else (Some res) @>

      let spgemmFun = Matrix.mxm context workGroupSize add mult

    //   testPropertyWithConfig config "omg" <| fun () ->
    //         printfn "Context = %A" context
    //         // let run =
    //         //     <@
    //         //         fun (ndRange: Range1D) (x: ClArray<byte>) (y: ClArray<byte>) (array: ClArray<byte>) ->
    //         //             let i = ndRange.GlobalID0
    //         //             let a = x.[i]
    //         //             let b = y.[i]
    //         //             let res = a * b
    //         //             let increase =
    //         //                 if res = 0uy
    //         //                 then None
    //         //                 else Some res
    //         //             match increase with
    //         //             | Some _ -> if i = 1 then array.[i] <- 1uy
    //         //             | _      -> ()

    //         //             match increase with
    //         //             Some _ -> if i = 1 then array.[i] <- array.[i] + 4uy
    //         //             | None   -> if i = 1 then array.[i] <- array.[i] + 32uy
    //         //     @>
    //         // let run =
    //         //     <@
    //         //         fun (ndRange: Range1D) (x: ClArray<byte>) (y: ClArray<byte>) (array: ClArray<byte>) ->
    //         //             let i = ndRange.GlobalID0
    //         //             let res = x.[i] * y.[i]
    //         //             let mutable increase = None
    //         //             if res = 0uy
    //         //             then increase <- None
    //         //             else increase <- Some res
    //         //             match increase with
    //         //             | Some _ -> if i = 1 then array.[i] <- 1uy
    //         //             | _      -> ()

    //         //             match increase with
    //         //             Some _ -> array.[i] <- array.[i] + 4uy
    //         //             | None   -> array.[i] <- array.[i] + 32uy
    //         //     @>
    //         let op =
    //             <@
    //                 fun x y ->
    //                     let mutable res = x * y

    //                     if res = 0uy then
    //                         // printf "AA"
    //                         None
    //                     else
    //                         // printf "BB"
    //                         (Some res)
    //             @>

    //         let run =
    //             <@
    //                 fun (ndRange: Range1D) (x: ClArray<byte>) (y: ClArray<byte>) (array: ClArray<byte>) ->
    //                     let mutable i = ndRange.GlobalID0
    //                     let a = x.[i]
    //                     let b = y.[i]
    //                     let increase = (%op) a b

    //                     match increase with
    //                     | Some v -> if i = 1 then array.[i] <- 1uy
    //                     | _      -> ()

    //                     let lid = ndRange.LocalID0

    //                     // let la = localArray<byte> 32
    //                     let laP = localArray<bool> 2
    //                     laP.[lid] <- false

    //                     let buff: byte option = None

    //                     match buff, increase with
    //                     | Some _, Some _ -> if i = 1 then array.[i] <- array.[i] + 4uy
    //                     | None,   Some _ -> if i = 1 then array.[i] <- array.[i] + 8uy
    //                     | Some _, None   -> if i = 1 then array.[i] <- array.[i] + 16uy
    //                     | None,   None   -> if i = 1 then array.[i] <- array.[i] + 32uy
    //             @>

    //         let program = context.Compile(run)
    //         let kernel = program.GetKernel()

    //         let array = context.CreateClArray<byte>(Array.create workGroupSize 0uy)
    //         let x = context.CreateClArray<byte>(Array.create workGroupSize 240uy)
    //         let y = context.CreateClArray<byte>(Array.create workGroupSize 112uy)

    //         let ndRange = Range1D.CreateValid(workGroupSize, workGroupSize)

    //         q.Post(
    //             Msg.MsgSetArguments
    //                 (fun () ->
    //                     kernel.KernelFunc
    //                         ndRange
    //                         x
    //                         y
    //                         array)
    //         )

    //         q.Post(Msg.CreateRunMsg<_, _>(kernel))

    //         let arr = q.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(array, Array.zeroCreate array.Length, ch))
    //         printfn "array.[1] = %A" arr.[1] ]



    //   testPropertyWithConfig config "byte" <| fun () ->
    //         let leftMatrix = Array2D.zeroCreate 1 2
    //         leftMatrix.[0, 0] <- 43uy
    //         leftMatrix.[0, 1] <- 240uy
    //         let rightMatrix = Array2D.zeroCreate 2 2
    //         rightMatrix.[0, 0] <- 234uy
    //         rightMatrix.[0, 1] <- 0uy
    //         rightMatrix.[1, 0] <- 78uy
    //         rightMatrix.[1, 1] <- 112uy
    //         let mask = Array2D.zeroCreate 1 2
    //         mask.[0, 0] <- true
    //         mask.[0, 1] <- true

    //         let isEqual = (=)
    //         let zero = 0uy
    //         let plus = (+)
    //         let mul = (*)

    //         let m1 =
    //             createMatrixFromArray2D CSR leftMatrix (isEqual zero)

    //         let m2 =
    //             createMatrixFromArray2D CSC rightMatrix (isEqual zero)

    //         if m1.NNZCount > 0 && m2.NNZCount > 0 then
    //             let expected =
    //                 Array2D.init
    //                 <| Array2D.length1 mask
    //                 <| Array2D.length2 mask
    //                 <| fun i j ->
    //                     if mask.[i, j] then
    //                         (leftMatrix.[i, *], rightMatrix.[*, j])
    //                         ||> Array.map2 mul
    //                         |> Array.reduce plus
    //                     else zero

    //             let expected =
    //                 createMatrixFromArray2D COO expected (isEqual zero)

    //             printfn "expected = %A" expected

    //             if expected.NNZCount > 0 then
    //                 let m1 = m1.ToBackend context
    //                 let m2 = m2.ToBackend context
    //                 let mask = Mask2D.FromArray2D(mask, not).ToBackend context

    //                 let result = spgemmFun q m1 m2 mask
    //                 let actual = Matrix.FromBackend q result

    //                 m1.Dispose q
    //                 m2.Dispose q
    //                 mask.Dispose q
    //                 result.Dispose q

    //                 // Check result
    //                 "Matrices should be equal"
    //                 |> Expect.equal actual expected ]


    //   let spgemmFun = Matrix.mxm context workGroupSize add mult
    //   makeTest context q 0uy (=) (+) (*) spgemmFun
    //   |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      let spgemmFun = Matrix.mxm context workGroupSize logicalOr logicalAnd
      makeTest context q false (=) (||) (&&) spgemmFun
      |> testPropertyWithConfig config (getCorrectnessTestName "bool") ]
    |> testList "SpGEMM tests"
