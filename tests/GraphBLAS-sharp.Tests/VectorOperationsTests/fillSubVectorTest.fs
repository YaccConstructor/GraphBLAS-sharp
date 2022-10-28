module Backend.Vector.FillSubVector

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests.Utils

let logger = Log.create "Vector.zeroCreate.Tests"

let clContext = defaultContext.ClContext

let checkResult
    (resultIsEqual: 'a -> 'a -> bool)
    (maskIsEqual: 'b -> 'b -> bool)
    vectorZero
    maskZero
    (actual: Vector<'a>)
    (leftVector: 'a [])
    (mask: 'b [])
    (value: 'a)
    =

    let expectedArrayLength = leftVector.Length

    let expectedArray =
        Array.create expectedArrayLength vectorZero

    for i in 0 .. expectedArrayLength - 1 do
        let resultItem =
            if maskIsEqual maskZero mask[i] then
                leftVector[i]
            else
                value

        expectedArray[i] <- resultItem

    match actual with
    | VectorCOO actual ->
        let actualArray = Array.create expectedArrayLength vectorZero

        for i in 0 .. actual.Indices.Length - 1 do
            actualArray[actual.Indices[i]] <- actual.Values[i]

        $"arrays must have the same values and length"
        |> compareArrays resultIsEqual actualArray expectedArray
    | _ -> failwith "Vector format must be COO."

let makeTest<'a, 'b when 'a: struct and 'b: struct>
    resultIsEqual
    maskIsEqual
    (vectorZero: 'a )
    (maskZero: 'b)
    (toCoo: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'a>)
    (fillVector: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'b> -> 'a -> ClVector<'a>)
    (maskFormat: VectorFormat)
    case
    (leftArray: 'a [])
    (maskArray: 'b [])
    (value: 'a)
    =

    if leftArray.Length > 0 then
        let q = case.ClContext.Queue
        let context = case.ClContext.ClContext

        let leftVector =
            createVectorFromArray case.FormatCase leftArray (resultIsEqual vectorZero)

        let clSourceVector =
            leftVector.ToDevice context

        let maskVector =
            createVectorFromArray maskFormat maskArray (maskIsEqual maskZero)

        let clMaskVector =
            maskVector.ToDevice context

        let clActual =
            fillVector q clSourceVector clMaskVector value

        let cooClActual = toCoo q clActual

        let actual = cooClActual.ToHost q

        clSourceVector.Dispose q
        clMaskVector.Dispose q
        clActual.Dispose q

        checkResult resultIsEqual maskIsEqual vectorZero maskZero actual leftArray maskArray value


let testFixtures (case: OperationCase<VectorFormat>) =
    let config = defaultConfig

    let getCorrectnessTestName datatype =
         sprintf "Correctness on %s, %A" datatype case.FormatCase

    let wgSize = 32
    let context = case.ClContext.ClContext

    [ let intFill = Vector.fillSubVector context wgSize

      let intToCoo = Vector.toCoo context wgSize

      case
      |> makeTest (=) (=) 0 0 intToCoo intFill VectorFormat.COO
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

       ]

let tests =
     testCases<VectorFormat>
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
    |> List.collect testFixtures
    |> testList "Backend.Vector.fillSubVector tests"
