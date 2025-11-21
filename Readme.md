[1 Cleaning Data in Pandas]
1. drop index colone,
2  delete row if age after today
3. if engineSize is 0.0 and FuelType is Nor electric then delet it
4. delete blancs (fuelType, transmision, FuelEfficiency)
5. acidentHistory delete other than No or Yes
6. Insurance delete other than Expired or Valid
7. RegistrationStatus delete other than complete or incomplete
8. split "carType" to [brand, model, version, class]
9. Doors, seat split celule, Capacity to int, >0 and <10
10. option : use this list of option to have a clomune with a bool for eatch type