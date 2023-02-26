module GraphBLAS.FSharp.Tests.Backend.Vector.Map2

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests.TestCases
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

let logger = Log.create "Vector.ElementWise.Tests"

let config = Utils.defaultConfig

let wgSize = Utils.defaultWorkGroupSize

let getCorrectnessTestName<'a> (case: OperationCase<'a>) dataType =
    $"Correctness on '{dataType} option -> '{dataType} option -> '{dataType} option, {case.Format}"

let checkResult isEqual resultZero (op: 'a -> 'b -> 'c) (actual: Vector<'c>) (leftArray: 'a []) (rightArray: 'b []) =

    let expectedArrayLength = leftArray.Length

    let expectedArray =
        Array.create expectedArrayLength resultZero

    for i in 0 .. expectedArrayLength - 1 do
        expectedArray.[i] <- op leftArray.[i] rightArray.[i]

    let expected =
        Utils.createVectorFromArray Dense expectedArray (isEqual resultZero)
        |> Utils.vectorToDenseVector

    match actual with
    | Vector.Dense actual ->
        "arrays must have the same values"
        |> Expect.equal actual expected
    | _ -> failwith "Vector format must be Sparse."

let correctnessGenericTest
    isEqual
    zero
    op
    (addFun: MailboxProcessor<_> -> AllocationFlag -> ClVector<'a> -> ClVector<'a> -> ClVector<'a>)
    (toDense: MailboxProcessor<_> -> AllocationFlag -> ClVector<'a> -> ClVector<'a>)
    case
    (leftArray: 'a [], rightArray: 'a [])
    =

    let isZero = (isEqual zero)

    let firstVectorHost =
        Utils.createVectorFromArray case.Format leftArray isZero

    let secondVectorHost =
        Utils.createVectorFromArray case.Format rightArray isZero

    if firstVectorHost.NNZ > 0
       && secondVectorHost.NNZ > 0 then

        let context = case.TestContext.ClContext
        let q = case.TestContext.Queue

        let firstVector = firstVectorHost.ToDevice context
        let secondVector = secondVectorHost.ToDevice context

        try
            let res =
                addFun q HostInterop firstVector secondVector

            firstVector.Dispose q
            secondVector.Dispose q

            let denseActual = toDense q HostInterop res

            let actual = denseActual.ToHost q

            res.Dispose q
            denseActual.Dispose q

            checkResult isEqual zero op actual leftArray rightArray
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | ex -> raise ex

let createTest case isEqual (zero: 'a) plus plusQ map2 =
    let context = case.TestContext.ClContext

    let map2 = map2 context plusQ wgSize

    let intToDense = Vector.toDense context wgSize

    case
    |> correctnessGenericTest isEqual zero plus map2 intToDense
    |> testPropertyWithConfig config (getCorrectnessTestName case $"%A{typeof<'a>}")

let addTestFixtures case =
    let context = case.TestContext.ClContext

    [ createTest case (=) 0 (+) ArithmeticOperations.intSum Vector.map2

      if Utils.isFloat64Available context.ClDevice then
          createTest case Utils.floatIsEqual 0.0 (+) ArithmeticOperations.floatSum Vector.map2

      createTest case Utils.float32IsEqual 0.0f (+) ArithmeticOperations.float32Sum Vector.map2
      createTest case (=) false (||) ArithmeticOperations.boolSum Vector.map2
      createTest case (=) 0uy (+) ArithmeticOperations.byteSum Vector.map2 ]

let addTests =
    operationGPUTests "Backend.Vector.Map2 add tests" addTestFixtures

let mulTestFixtures case =
    let context = case.TestContext.ClContext

    [ createTest case (=) 0 (*) ArithmeticOperations.intMul Vector.map2

      if Utils.isFloat64Available context.ClDevice then
          createTest case Utils.floatIsEqual 0.0 (*) ArithmeticOperations.floatMul Vector.map2

      createTest case Utils.float32IsEqual 0.0f (*) ArithmeticOperations.float32Mul Vector.map2
      createTest case (=) false (&&) ArithmeticOperations.boolMul Vector.map2
      createTest case (=) 0uy (*) ArithmeticOperations.byteMul Vector.map2 ]

let mulTests =
    operationGPUTests "Backend.Vector.map2 mul tests" addTestFixtures

let addAtLeastOneTestFixtures case =
    let context = case.TestContext.ClContext

    [ createTest case (=) 0 (+) ArithmeticOperations.intSumAtLeastOne Vector.map2AtLeastOne

      if Utils.isFloat64Available context.ClDevice then
          createTest case Utils.floatIsEqual 0.0 (+) ArithmeticOperations.floatSumAtLeastOne Vector.map2AtLeastOne

      createTest case Utils.float32IsEqual 0.0f (+) ArithmeticOperations.float32SumAtLeastOne Vector.map2AtLeastOne
      createTest case (=) false (||) ArithmeticOperations.boolSumAtLeastOne Vector.map2AtLeastOne
      createTest case (=) 0uy (+) ArithmeticOperations.byteSumAtLeastOne Vector.map2AtLeastOne ]

let addAtLeastOneTests =
    operationGPUTests "Backend.Vector.Map2LeastOne add tests" addTestFixtures

let mulAtLeastOneTestFixtures case =
    let context = case.TestContext.ClContext

    [ createTest case (=) 0 (*) ArithmeticOperations.intMulAtLeastOne Vector.map2AtLeastOne

      if Utils.isFloat64Available context.ClDevice then
          createTest case Utils.floatIsEqual 0.0 (*) ArithmeticOperations.floatMulAtLeastOne Vector.map2AtLeastOne

      createTest case Utils.float32IsEqual 0.0f (*) ArithmeticOperations.float32MulAtLeastOne Vector.map2AtLeastOne
      createTest case (=) false (&&) ArithmeticOperations.boolMulAtLeastOne Vector.map2AtLeastOne
      createTest case (=) 0uy (*) ArithmeticOperations.byteMulAtLeastOne Vector.map2AtLeastOne ]

let mulAtLeastOneTests =
    operationGPUTests "Backend.Vector.Map2AtLeasOne mul tests" mulTestFixtures

let addGeneralTestFixtures (case: OperationCase<VectorFormat>) =
    let context = case.TestContext.ClContext

    [ createTest case (=) 0 (+) ArithmeticOperations.intSum Vector.map2General

      if Utils.isFloat64Available context.ClDevice then
          createTest case Utils.floatIsEqual 0.0 (+) ArithmeticOperations.floatSum Vector.map2General

      createTest case Utils.float32IsEqual 0.0f (+) ArithmeticOperations.float32Sum Vector.map2General
      createTest case (=) false (||) ArithmeticOperations.boolSum Vector.map2General
      createTest case (=) 0uy (+) ArithmeticOperations.byteSum Vector.map2General ]

let addGeneralTests =
    operationGPUTests "Backend.Vector.Map2Gen add tests" addGeneralTestFixtures

let mulGeneralTestFixtures case =
    let context = case.TestContext.ClContext

    [ createTest case (=) 0 (*) ArithmeticOperations.intMul Vector.map2General

      if Utils.isFloat64Available context.ClDevice then
          createTest case Utils.floatIsEqual 0.0 (*) ArithmeticOperations.floatMul Vector.map2General

      createTest case Utils.float32IsEqual 0.0f (*) ArithmeticOperations.float32Mul Vector.map2General
      createTest case (=) false (&&) ArithmeticOperations.boolMul Vector.map2General
      createTest case (=) 0uy (*) ArithmeticOperations.byteMul Vector.map2General ]

let fillSubVectorComplementedQ<'a, 'b> value =
    <@ fun (left: 'a option) (right: 'b option) ->
        match left with
        | None -> Some value
        | _ -> right @>

let fillSubVectorFun value zero isEqual =
    fun left right ->
        if isEqual left zero then
            value
        else
            right

let mulGeneralTests =
    operationGPUTests "Backend.Vector.SparseVector.map2Gen mul tests" mulGeneralTestFixtures

let complementedGeneralTestFixtures case =
    let context = case.TestContext.ClContext

    [ createTest case (=) 0 (fillSubVectorFun 1 0 (=)) (fillSubVectorComplementedQ 1) Vector.map2General

      if Utils.isFloat64Available context.ClDevice then
          createTest
              case
              Utils.floatIsEqual
              0.0
              (fillSubVectorFun 1.0 0.0 Utils.floatIsEqual)
              (fillSubVectorComplementedQ 1.0)
              Vector.map2General

      createTest
          case
          Utils.float32IsEqual
          0.0f
          (fillSubVectorFun 1.0f 0.0f Utils.float32IsEqual)
          (fillSubVectorComplementedQ 1.0f)
          Vector.map2General

      createTest case (=) false (fillSubVectorFun true false (=)) (fillSubVectorComplementedQ true) Vector.map2General

      createTest case (=) 0uy (fillSubVectorFun 1uy 0uy (=)) (fillSubVectorComplementedQ 1uy) Vector.map2General ]


let complementedGeneralTests =
    operationGPUTests "Backend.Vector.Map2Gen mask tests" complementedGeneralTestFixtures
