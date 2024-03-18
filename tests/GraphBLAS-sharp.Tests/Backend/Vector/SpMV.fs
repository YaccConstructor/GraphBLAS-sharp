module GraphBLAS.FSharp.Tests.Backend.Vector.SpMV

open Expecto
open Microsoft.FSharp.Collections
open Microsoft.FSharp.Core
open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Tests.TestCases
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Quotes

let config = Utils.defaultConfig

let wgSize = Constants.Common.defaultWorkGroupSize

let checkResult isEqual sumOp mulOp zero (baseMtx: 'a [,]) (baseVtr: 'a []) (actual: 'a option []) =
    let rows = Array2D.length1 baseMtx
    let columns = Array2D.length2 baseMtx

    let expected = Array.create rows zero

    for i in 0 .. rows - 1 do
        let mutable sum = zero

        for v in 0 .. columns - 1 do
            sum <- sumOp sum (mulOp baseMtx.[i, v] baseVtr.[v])

        expected.[i] <- sum

    for i in 0 .. actual.Size - 1 do
        match actual.[i] with
        | Some v ->
            if isEqual zero v then
                failwith "Resulting zeroes should be implicit."
        | None -> ()

    for i in 0 .. actual.Size - 1 do
        match actual.[i] with
        | Some v ->
            Expect.isTrue (isEqual v expected.[i]) $"Values should be the same. Actual is {v}, expected {expected.[i]}."
        | None ->
            Expect.isTrue
                (isEqual zero expected.[i])
                $"Values should be the same. Actual is {zero}, expected {expected.[i]}."

let correctnessGenericTest
    zero
    sumOp
    mulOp
    (spMV: MailboxProcessor<_> -> AllocationFlag -> ClMatrix<'a> -> ClVector<'a> -> ClVector<'a>)
    (isEqual: 'a -> 'a -> bool)
    q
    (testContext: TestContext)
    (matrix: 'a [,], vector: 'a [], _: bool [])
    =

    let mtx =
        Utils.createMatrixFromArray2D CSR matrix (isEqual zero)

    let vtr =
        Utils.createVectorFromArray Dense vector (isEqual zero)

    if mtx.NNZ > 0 && vtr.Size > 0 then
        try
            let m = mtx.ToDevice testContext.ClContext

            let v = vtr.ToDevice testContext.ClContext

            let res = spMV testContext.Queue HostInterop m v

            m.Dispose q
            v.Dispose q

            match res with
            | ClVector.Dense res ->
                let hostRes = res.ToHostAndFree q

                checkResult isEqual sumOp mulOp zero matrix vector hostRes
            | _ -> failwith "Impossible"
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | ex -> raise ex

let createTest testContext (zero: 'a) isEqual add mul addQ mulQ =
    let context = testContext.ClContext
    let q = testContext.Queue

    let getCorrectnessTestName datatype =
        $"Correctness on %s{datatype}, %A{testContext.ClContext}"

    let spMV = Operations.SpMV addQ mulQ context wgSize

    testContext
    |> correctnessGenericTest zero add mul spMV isEqual q
    |> testPropertyWithConfig config (getCorrectnessTestName $"{typeof<'a>}")


let testFixturesSpMV (testContext: TestContext) =
    [ let context = testContext.ClContext
      let q = testContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      createTest testContext false (=) (||) (&&) ArithmeticOperations.boolSumOption ArithmeticOperations.boolMulOption
      createTest testContext 0 (=) (+) (*) ArithmeticOperations.intSumOption ArithmeticOperations.intMulOption

      if Utils.isFloat64Available context.ClDevice then
          createTest
              testContext
              0.0
              Utils.floatIsEqual
              (+)
              (*)
              ArithmeticOperations.floatSumOption
              ArithmeticOperations.floatMulOption

      createTest
          testContext
          0.0f
          Utils.float32IsEqual
          (+)
          (*)
          ArithmeticOperations.float32SumOption
          ArithmeticOperations.float32MulOption

      createTest testContext 0uy (=) (+) (*) ArithmeticOperations.byteSumOption ArithmeticOperations.byteMulOption ]

let tests =
    gpuTests "Backend.Vector.SpMV tests" testFixturesSpMV
