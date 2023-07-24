module GraphBLAS.FSharp.Tests.Backend.Vector.Map

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Microsoft.FSharp.Collections
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Backend
open GraphBLAS.FSharp.Tests.TestCases
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open Mono.CompilerServices.SymbolWriter

let logger = Log.create "Vector.Map.Tests"

let config = Utils.defaultConfig
let wgSize = Utils.defaultWorkGroupSize

let getCorrectnessTestName case datatype =
    $"Correctness on %s{datatype}, %A{case}"

let checkResult isEqual op zero (baseVector: 'a []) (actual: Vector<'b>) =

    let expectedArrayLength = baseVector.Length

    let expectedArray =
        Array.create expectedArrayLength zero

    for i in 0 .. expectedArrayLength - 1 do
        expectedArray.[i] <- op baseVector.[i]

    let expected =
        Utils.createVectorFromArray Dense expectedArray (isEqual zero)
        |> Utils.vectorToDenseVector

    match actual with
    | Vector.Dense actual ->
        "arrays must have the same values"
        |> Expect.equal actual expected
    | _ -> failwith "Vector format must be Sparse."

let correctnessGenericTest
    zero
    op
    (addFun: MailboxProcessor<_> -> AllocationFlag -> ClVector<'a> -> ClVector<'a>)
    (toDense: MailboxProcessor<_> -> AllocationFlag -> ClVector<'a> -> ClVector<'a>)
    (isEqual: 'a -> 'a -> bool)
    (case: OperationCase<VectorFormat>)
    (array: 'a [])
    =

    let isZero = (isEqual zero)

    let vectorHost =
        Utils.createVectorFromArray case.Format array isZero

    if vectorHost.NNZ > 0 then

        let context = case.TestContext.ClContext
        let q = case.TestContext.Queue

        let vector = vectorHost.ToDevice context

        try
            let res =
                addFun q HostInterop vector

            vector.Dispose q

            let denseActual = toDense q HostInterop res

            let actual = denseActual.ToHost q

            res.Dispose q
            denseActual.Dispose q

            checkResult isEqual op zero array actual
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | ex -> raise ex

let createTestMap case (zero: 'a) (constant: 'a) binOp isEqual opQ =
    let getCorrectnessTestName = getCorrectnessTestName case

    let context = case.TestContext.ClContext
    let q = case.TestContext.Queue

    let unaryOp = binOp constant
    let unaryOpQ = opQ zero constant

    let map =
        Operations.Vector.map unaryOpQ context wgSize

    let toDense = Vector.toDense context wgSize

    case
    |> correctnessGenericTest zero unaryOp map toDense isEqual
    |> testPropertyWithConfig config (getCorrectnessTestName $"{typeof<'a>}")

let testFixturesMapNot case =
    [ let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      createTestMap case false true (fun _ -> not) (=) (fun _ _ -> ArithmeticOperations.notOption) ]

let notTests =
    operationGPUTests "not" testFixturesMapNot

let testFixturesMapAdd case =
    [ let context = case.TestContext.ClContext
      let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      createTestMap case 0 10 (+) (=) ArithmeticOperations.addLeftConst

      if Utils.isFloat64Available context.ClDevice then
          createTestMap case 0.0 10.0 (+) Utils.floatIsEqual ArithmeticOperations.addLeftConst

      createTestMap case 0.0f 10.0f (+) Utils.float32IsEqual ArithmeticOperations.addLeftConst

      createTestMap case 0uy 10uy (+) (=) ArithmeticOperations.addLeftConst ]

let addTests =
    operationGPUTests "add" testFixturesMapAdd

let testFixturesMapMul case =
    [ let context = case.TestContext.ClContext
      let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      createTestMap case 0 10 (*) (=) ArithmeticOperations.mulLeftConst

      if Utils.isFloat64Available context.ClDevice then
          createTestMap case 0.0 10.0 (*) Utils.floatIsEqual ArithmeticOperations.mulLeftConst

      createTestMap case 0.0f 10.0f (*) Utils.float32IsEqual ArithmeticOperations.mulLeftConst

      createTestMap case 0uy 10uy (*) (=) ArithmeticOperations.mulLeftConst ]

let mulTests =
    operationGPUTests "mul" testFixturesMapMul

let allTests =
    testList "Map" [ addTests; mulTests; notTests ]
