module GraphBLAS.FSharp.Tests.Backend.Vector.Map2

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests.TestCases
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions

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
    (addFun: MailboxProcessor<_> -> AllocationFlag -> ClVector<'a> -> ClVector<'a> -> ClVector<'a> option)
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

            match res with
            | Some res ->
                let denseActual = toDense q HostInterop res

                let actual = denseActual.ToHost q

                res.Dispose q
                denseActual.Dispose q

                checkResult isEqual zero op actual leftArray rightArray
            | _ -> ()

            firstVector.Dispose q
            secondVector.Dispose q
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | ex -> raise ex

let createTest case isEqual (zero: 'a) plus plusQ map2 =
    let context = case.TestContext.ClContext

    let map2 = map2 plusQ context wgSize

    let intToDense = Vector.toDense context wgSize

    case
    |> correctnessGenericTest isEqual zero plus map2 intToDense
    |> testPropertyWithConfig config (getCorrectnessTestName case $"%A{typeof<'a>}")

let addTestFixtures case =
    let context = case.TestContext.ClContext

    [ createTest case (=) 0 (+) ArithmeticOperations.intSumOption Operations.Vector.map2

      if Utils.isFloat64Available context.ClDevice then
          createTest case Utils.floatIsEqual 0.0 (+) ArithmeticOperations.floatSumOption Operations.Vector.map2

      createTest case Utils.float32IsEqual 0.0f (+) ArithmeticOperations.float32SumOption Operations.Vector.map2
      createTest case (=) false (||) ArithmeticOperations.boolSumOption Operations.Vector.map2
      createTest case (=) 0uy (+) ArithmeticOperations.byteSumOption Operations.Vector.map2 ]

let addTests = operationGPUTests "add" addTestFixtures

let mulTestFixtures case =
    let context = case.TestContext.ClContext

    [ createTest case (=) 0 (*) ArithmeticOperations.intMulOption Operations.Vector.map2

      if Utils.isFloat64Available context.ClDevice then
          createTest case Utils.floatIsEqual 0.0 (*) ArithmeticOperations.floatMulOption Operations.Vector.map2

      createTest case Utils.float32IsEqual 0.0f (*) ArithmeticOperations.float32MulOption Operations.Vector.map2
      createTest case (=) false (&&) ArithmeticOperations.boolMulOption Operations.Vector.map2
      createTest case (=) 0uy (*) ArithmeticOperations.byteMulOption Operations.Vector.map2 ]

let mulTests = operationGPUTests "mul" addTestFixtures

let addAtLeastOneTestFixtures case =
    let context = case.TestContext.ClContext

    [ createTest case (=) 0 (+) ArithmeticOperations.intSumAtLeastOne Operations.Vector.map2AtLeastOne

      if Utils.isFloat64Available context.ClDevice then
          createTest
              case
              Utils.floatIsEqual
              0.0
              (+)
              ArithmeticOperations.floatSumAtLeastOne
              Operations.Vector.map2AtLeastOne

      createTest
          case
          Utils.float32IsEqual
          0.0f
          (+)
          ArithmeticOperations.float32SumAtLeastOne
          Operations.Vector.map2AtLeastOne
      createTest case (=) false (||) ArithmeticOperations.boolSumAtLeastOne Operations.Vector.map2AtLeastOne
      createTest case (=) 0uy (+) ArithmeticOperations.byteSumAtLeastOne Operations.Vector.map2AtLeastOne ]

let addAtLeastOneTests =
    operationGPUTests "addAtLeastOne" addTestFixtures

let mulAtLeastOneTestFixtures case =
    let context = case.TestContext.ClContext

    [ createTest case (=) 0 (*) ArithmeticOperations.intMulAtLeastOne Operations.Vector.map2AtLeastOne

      if Utils.isFloat64Available context.ClDevice then
          createTest
              case
              Utils.floatIsEqual
              0.0
              (*)
              ArithmeticOperations.floatMulAtLeastOne
              Operations.Vector.map2AtLeastOne

      createTest
          case
          Utils.float32IsEqual
          0.0f
          (*)
          ArithmeticOperations.float32MulAtLeastOne
          Operations.Vector.map2AtLeastOne
      createTest case (=) false (&&) ArithmeticOperations.boolMulAtLeastOne Operations.Vector.map2AtLeastOne
      createTest case (=) 0uy (*) ArithmeticOperations.byteMulAtLeastOne Operations.Vector.map2AtLeastOne ]

let mulAtLeastOneTests =
    operationGPUTests "mulAtLeastOne" mulTestFixtures

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

let complementedGeneralTestFixtures case =
    let context = case.TestContext.ClContext

    [ createTest case (=) 0 (fillSubVectorFun 1 0 (=)) (fillSubVectorComplementedQ 1) Operations.Vector.map2

      if Utils.isFloat64Available context.ClDevice then
          createTest
              case
              Utils.floatIsEqual
              0.0
              (fillSubVectorFun 1.0 0.0 Utils.floatIsEqual)
              (fillSubVectorComplementedQ 1.0)
              Operations.Vector.map2

      createTest
          case
          Utils.float32IsEqual
          0.0f
          (fillSubVectorFun 1.0f 0.0f Utils.float32IsEqual)
          (fillSubVectorComplementedQ 1.0f)
          Operations.Vector.map2

      createTest
          case
          (=)
          false
          (fillSubVectorFun true false (=))
          (fillSubVectorComplementedQ true)
          Operations.Vector.map2

      createTest case (=) 0uy (fillSubVectorFun 1uy 0uy (=)) (fillSubVectorComplementedQ 1uy) Operations.Vector.map2 ]


let complementedGeneralTests =
    operationGPUTests "mask" complementedGeneralTestFixtures

let allTests =
    testList
        "Map"
        [ addTests
          mulTests
          addAtLeastOneTests
          mulAtLeastOneTests
          complementedGeneralTests ]
