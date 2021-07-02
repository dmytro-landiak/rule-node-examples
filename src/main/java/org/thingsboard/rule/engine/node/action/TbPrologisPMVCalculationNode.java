package org.thingsboard.rule.engine.node.action;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.analysis.solvers.BrentSolver;
import org.thingsboard.rule.engine.api.EmptyNodeConfiguration;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.concurrent.ExecutionException;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "prologis pmv calculation",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "tbPrologisPMVCalculationNodeConfig")
public class TbPrologisPMVCalculationNode implements TbNode {
    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {

    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        ctx.tellSuccess(msg);
    }

    // for ASHRAE 55 standart
    private double calculatePMVAshrae(double airTemp, double meanRadiantTemp,
                                      double airSpeed, double relativeHumidity,
                                      double metabolicRate, double clothesInsulation, double externalWork) {
        double ce = getCoolingEffect(airTemp, meanRadiantTemp, getRelativeSpeed(airSpeed, metabolicRate), relativeHumidity, metabolicRate, clothesInsulation, externalWork);
        return calculatePMV(airTemp - ce, meanRadiantTemp - ce, 0.1, relativeHumidity, metabolicRate, clothesInsulation, externalWork);
    }

    private double getCoolingEffect(double airTemp, double meanRadiantTemp,
                                    double relativeAirSpeed, double relativeHumidity,
                                    double metabolicRate, double clothesInsulation, double externalWork) {
        double stillAirThreshold = 0.1;
        if (relativeAirSpeed <= 0.1) {
            return 0;
        }
        double initialSetTmp = getStandardEffectiveTemp(airTemp, meanRadiantTemp, relativeAirSpeed, relativeHumidity, metabolicRate, clothesInsulation, externalWork);

        BrentSolver brentSolver = new BrentSolver(0.001, 0.001);
        double ce = brentSolver.solve(1000000, x -> getStandardEffectiveTemp(airTemp - x, meanRadiantTemp - x,
                stillAirThreshold, relativeHumidity, metabolicRate, clothesInsulation, externalWork) - initialSetTmp, 0.0, 15);
        if (ce == 0) {
            log.warn("The cooling effect could not be calculated, assuming ce = 0");
        }
        return Math.round(ce * 100) / 100.0;
    }

    // for ISO 7730 standart
    private double calculatePMV(double airTemp, double meanRadiantTemp,
                                double relativeAirSpeed, double relativeHumidity,
                                double metabolicRate, double clothesInsulation,
                                double externalWork) {
        //externalWork -> default value 0
        double pa = relativeHumidity * 10 * Math.exp(16.6536 - 4030.183 / (airTemp + 235)); //Partial water vapour pressure
        double closesInsulationInWM2 = clothesInsulation * 0.155; //thermal insulation of the clothing in M2K/W
        double metabolicRateInWM2 = metabolicRate * 58.15;  //metabolic rate in W/M2
        double externalWorkInWM2 = externalWork * 58.15;    //external work in W/M2
        double heatProductionInHumanBody = metabolicRateInWM2 - externalWorkInWM2; //internal heat production in the human body

        // calculation of the clothing area factor
        double clothesFactor = closesInsulationInWM2 <= 0.078  //ratio of surface clothed body over nude body
                ? 1 + (1.29 * closesInsulationInWM2)
                : 1.05 + (0.0645 * closesInsulationInWM2);

        //heat transfer coefficient by forced convection
        double forcedHeatTransferCoeff = 12.1 * Math.sqrt(relativeAirSpeed);
        double hc = forcedHeatTransferCoeff;
        double taa = airTemp + 273;
        double tra = meanRadiantTemp + 273;
        double tCla = taa + (35.5 - airTemp) / (3.5 * closesInsulationInWM2 + 0.1);
        double p1 = closesInsulationInWM2 * clothesFactor;
        double p2 = p1 * 3.96;
        double p3 = p1 * 100;
        double p4 = p1 * taa;
        double p5 = (308.7 - 0.028 * heatProductionInHumanBody) + (p2 * Math.pow(tra / 100.0, 4));
        double xn = tCla / 100;
        double xf = tCla / 50;


        for (int i = 0; i <= 150 || Math.abs(xn - xf) > 0.00015; i++) {
            xf = (xf + xn) / 2;
            double hcn = 2.38 * Math.pow(Math.abs(100.0 * xf - taa), 0.25);
            hc = Math.max(forcedHeatTransferCoeff, hcn);
            xn = (p5 + p4 * hc - p2 * Math.pow(xf, 4)) / (100 + p3 * hc);
        }
        double tcl = 100 * xn - 273;

        //heat loss diff. through skin
        double hl1 = 3.05 * 0.001 * (5733 - (6.99 * heatProductionInHumanBody) - pa);

        //heat loss by sweating
        double hl2 = heatProductionInHumanBody > 58.15
                ? 0.42 * (heatProductionInHumanBody - 58.15)
                : 0;
        //latent respiration heat loss
        double hl3 = 1.7 * 0.00001 * metabolicRateInWM2 * (5867 - pa);
        //dry respiration heat loss
        double hl4 = 0.0014 * metabolicRateInWM2 * (34 - airTemp);
        //heat loss by radiation
        double hl5 = 3.96 * clothesFactor * (Math.pow(xn, 4) - Math.pow(tra / 100.0, 4));
        //heat loss by convection
        double hl6 = clothesFactor * hc * (tcl - airTemp);

        double pmv = (0.303 * Math.exp(-0.036 * metabolicRateInWM2) + 0.028) * (heatProductionInHumanBody - hl1 - hl2 - hl3 - hl4 - hl5 - hl6);
        return Math.round(pmv * 100.0) / 100.0;
    }

    private double getStandardEffectiveTemp(double airTemp, double meantRadiantTemp,
                                                     double airSpeed, double relativeHumidity,
                                                     double metabolicRate, double clothesInsulation, double externalWork) {
        double vaporPressure = relativeHumidity * getSaturationVaporPressure(airTemp) / 100;
        double bodySurfaceArea = 1.8258;
        double atmosphericPressure = 101325;
        //Initial variables as defined in the ASHRAE 55-2017
        airSpeed = Math.max(airSpeed, 0.1);
        double kClo = 0.25;
        double bodyWeight = 69.9;
        double metFactor = 58.2;
        double sbc = 0.000000056697;  // Stefan-Boltzmann constant (W/m2K4)
        double sweatingCoeff = 170;  //driving coefficient for regulatory sweating
        double vasodilationCoeff = 200;  //driving coefficient for vasodilation
        double vasoconstrictionCoeff = 0.5;  //driving coefficient for vasoconstriction

        double skinTempNeutral = 33.7;
        double coreTempNeutral = 36.8;
        double bodyTempNeutral = 36.49;
        double skinBloodFlowNeutral = 6.3;

        double skinTemp = skinTempNeutral;
        double coreTemp = coreTempNeutral;
        double skinBloodFlow = skinBloodFlowNeutral;

        //initialize some variables
        double dry = 0;
        double skinWetness = 0;
        double skinMass = 0.1;  //fractional skin mass
        double totalEvaporativeHeatLoss = 0.1 * metabolicRate;  //total evaporative heat loss, W

        double pressureInAtmospheres = atmosphericPressure / 101325;

        double thermalClothesResistance = 0.155 * clothesInsulation;  //thermal resistance of clothing, °C M^2 /W
        double bodySurfaceAreaIncrease = 1.0 + 0.15 * clothesInsulation;  //increase in body surface area due to clothing
        double lewisRatio = 2.2 / pressureInAtmospheres;  //Lewis ratio

        //metabolic rate
        double rm = metabolicRate * metFactor;
        double m = metabolicRate * metFactor;

        double evaporativeEfficiency = clothesInsulation <= 0 ? 0.38 * Math.pow(airSpeed, -0.29) : 0.59 * Math.pow(airSpeed, -0.08);
        double efficiencyOfWatherPermeation = clothesInsulation <= 0 ? 1.0 : 0.45;

        double forcedHeatTransferCoeff = 8.600001 * Math.pow((airSpeed * pressureInAtmospheres), 0.53);    //forced convective heat transfer coefficient, W/(m2 °C)
        double correctedHeatTransferCoeff = Math.max(3.0 * Math.pow(pressureInAtmospheres, 0.53), forcedHeatTransferCoeff); //corrected convective heat transfer coefficient

        double linearizedRadiactiveHeatTransferCoeff = 4.7;  //linearized radiative heat transfer coefficient
        double sumOfConvectiveAndRadiantCoeff = linearizedRadiactiveHeatTransferCoeff + correctedHeatTransferCoeff;  // sum of convective and radiant heat transfer coefficient W/(m2*K)
        double resistanceOfAirLayer = 1 / (bodySurfaceAreaIncrease * sumOfConvectiveAndRadiantCoeff);  // resistance of air layer to dry heat
        double operativeTemp = (linearizedRadiactiveHeatTransferCoeff * meantRadiantTemp + correctedHeatTransferCoeff * airTemp) / sumOfConvectiveAndRadiantCoeff;  // operative temperature

        for (int i = 0; i < 60; i++) {
            // temperature of the outer surface of clothing
            double clothesTemp = (resistanceOfAirLayer * skinTemp + thermalClothesResistance * operativeTemp) / (resistanceOfAirLayer + thermalClothesResistance);  // initial guess
            for (int j = 0; j <= 150; j++) {
                //0.72 in the following equation is the ratio of A_r/A_body see eq 35 ASHRAE fund 2017
                linearizedRadiactiveHeatTransferCoeff = 4.0 * sbc * Math.pow((clothesTemp + meantRadiantTemp) / 2 + 273.15, 3) * 0.72;
                sumOfConvectiveAndRadiantCoeff = linearizedRadiactiveHeatTransferCoeff + correctedHeatTransferCoeff;
                resistanceOfAirLayer = 1 / (bodySurfaceAreaIncrease * sumOfConvectiveAndRadiantCoeff);
                operativeTemp = (linearizedRadiactiveHeatTransferCoeff * meantRadiantTemp + correctedHeatTransferCoeff * airTemp) / sumOfConvectiveAndRadiantCoeff;
                double clothesTempNew = (resistanceOfAirLayer * skinTemp + thermalClothesResistance * operativeTemp) / (resistanceOfAirLayer + thermalClothesResistance);
                if (Math.abs(clothesTempNew - clothesTemp) <= 0.01) {
                    break;
                }
                clothesTemp = clothesTempNew;
            }

            dry = (skinTemp - operativeTemp) / (resistanceOfAirLayer + thermalClothesResistance); //total sensible heat loss, W
            double energyTransportRate = (coreTemp - skinTemp) * (5.28 + 1.163 * skinBloodFlow); //rate of energy transport between core and skin, W
            double heatLossByRespiration = 0.0023 * m * (44 - vaporPressure);   //latent heat loss due to respiration
            double rateOfHeatLossByRespiration = 0.0014 * m * (34 - airTemp);  //rate of convective heat loss from respiration, W/m2
            double rateOfEnergyStorageInCore = m - energyTransportRate - heatLossByRespiration - rateOfHeatLossByRespiration - externalWork;  //rate of energy storage in the core
            double rateOfEnergyStorageInSkin = energyTransportRate - dry - totalEvaporativeHeatLoss;  //rate of energy storage in the skin
            double thermalCapacitySkin = 0.97 * skinMass * bodyWeight;  // thermal capacity skin
            double thermalCapacityCore = 0.97 * (1 - skinMass) * bodyWeight;  // thermal capacity core
            double rateOfChangeSkinTempPerMinute = (rateOfEnergyStorageInSkin * bodySurfaceArea) / (thermalCapacitySkin * 60); //rate of change skin temperature °C per minute
            double rateOfChangeCoreTempPerMinute = rateOfEnergyStorageInCore * bodySurfaceArea / (thermalCapacityCore * 60);  // rate of change core temperature °C per minute
            skinTemp = skinTemp + rateOfChangeSkinTempPerMinute;
            coreTemp = coreTemp + rateOfChangeCoreTempPerMinute;
            double meanBodyTemp = skinMass * skinTemp + (1 - skinMass) * coreTemp;  // mean body temperature, °C
            double thermoregulatoryControlSignalFromSkin = skinTemp - skinTempNeutral; //thermoregulatory control signal from the skin
            double vasodilatixonSignal = thermoregulatoryControlSignalFromSkin > 0 ? thermoregulatoryControlSignalFromSkin : 0; //vasodilation signal
            double vasoconstrictionSignal = (-1 * thermoregulatoryControlSignalFromSkin) > 0 ? -1 * thermoregulatoryControlSignalFromSkin : 0;  //vasoconstriction signal
            double thermoregulatoryControlSignalFromSkinCore = coreTemp - coreTempNeutral; //thermoregulatory control signal from the skin, °C
            double vasodilationSignalCore = thermoregulatoryControlSignalFromSkinCore > 0 ? thermoregulatoryControlSignalFromSkinCore : 0;  //vasodilation signal
            double vasoconstrictionSignalCore = (-1.0 * thermoregulatoryControlSignalFromSkinCore) > 0 ? -1.0 * thermoregulatoryControlSignalFromSkinCore : 0; //vasoconstriction signal
            double bodySignal = meanBodyTemp - bodyTempNeutral;
            double vasodilationSignalBody = bodySignal > 0 ? bodySignal : 0;
            skinBloodFlow = (skinBloodFlowNeutral + vasodilationCoeff * vasodilationSignalCore) / (1 + vasoconstrictionCoeff * vasoconstrictionSignal);
            skinBloodFlow = Math.min(90, skinBloodFlow);
            skinBloodFlow = Math.max(skinBloodFlow, 0.5);
            double regulatotySweating = Math.min(500, sweatingCoeff * vasodilationSignalBody * Math.exp(vasodilatixonSignal / 10.7));
            double heatLostByVaporizationSweat = 0.68 * regulatotySweating; //heat lost by vaporization sweat
            double evaporativeResistanceAirLayer = 1 / (lewisRatio * bodySurfaceAreaIncrease * correctedHeatTransferCoeff);  //evaporative resistance air layer
            double rEcl = thermalClothesResistance / (lewisRatio * efficiencyOfWatherPermeation);
            double evaporativeCapacityMax = (Math.exp(18.6686 - 4030.183 / (skinTemp + 235)) - vaporPressure) / (evaporativeResistanceAirLayer + rEcl);
            double heatLossRatio = heatLostByVaporizationSweat / evaporativeCapacityMax;  // ratio heat loss sweating to max heat loss sweating
            skinWetness = 0.06 + 0.94 * heatLossRatio;  //skin wetness
            double vaporDiffusionThroughSkin = skinWetness * evaporativeCapacityMax - heatLostByVaporizationSweat; // vapor diffusion through skin
            if (skinWetness > evaporativeEfficiency) {
                skinWetness = evaporativeEfficiency;
                heatLossRatio = evaporativeEfficiency / 0.94;
                heatLostByVaporizationSweat = heatLossRatio * evaporativeCapacityMax;
                vaporDiffusionThroughSkin = 0.06 * (1.0 - heatLossRatio) * evaporativeCapacityMax;
            }
            if (evaporativeCapacityMax < 0) {
                vaporDiffusionThroughSkin = 0;
                heatLostByVaporizationSweat = 0;
                skinWetness = evaporativeEfficiency;
            }
            totalEvaporativeHeatLoss = heatLostByVaporizationSweat + vaporDiffusionThroughSkin;  // total evaporative heat loss sweating and vapor diffusion
            m = rm
                    + (19.4 * vasoconstrictionSignal * vasoconstrictionSignalCore); // metabolicRate shivering W/m2
            skinMass = 0.0417737 + 0.7451833 / (skinBloodFlow + 0.585417);
        }

        double totalHeatLossFromSkin = dry + totalEvaporativeHeatLoss;  // total heat loss from skin, W
        double w = skinWetness;
        double saturationVapourPressureOfWaterOfSkin = Math.exp(18.6686 - 4030.183 / (skinTemp + 235));    //saturation vapour pressure of water of the skin
        double radiativeHeatTransferCoeff = linearizedRadiactiveHeatTransferCoeff;  // standard environment radiative heat transfer coefficient

        double convectiveHeatTransferCoeff = Math.max(3 * Math.pow(pressureInAtmospheres, 0.53), 3);

        double sumOfConvectiveAndRadiativeHeatTransferCoeff = convectiveHeatTransferCoeff + radiativeHeatTransferCoeff; // sum of convective and radiant heat transfer coefficient W/(m2*K)
        double thermalResistanceOfClothing = 1.52 / ((metabolicRate - externalWork / metFactor) + 0.6944) - 0.1835;  //thermal resistance of clothing, °C M^2 /W
        double thermalInsulationOfClothing = 0.155 * thermalResistanceOfClothing;  // thermal insulation of the clothing in M2K/W
        double increaseInBodySurfaceArea = 1 + kClo * thermalResistanceOfClothing;  //increase in body surface area due to clothing
        double ratioOfSurfacedClothedBodyOverNudeBody = 1 / (1 + 0.155 * increaseInBodySurfaceArea *
                sumOfConvectiveAndRadiativeHeatTransferCoeff * thermalResistanceOfClothing) ; // ratio of surface clothed body over nude body
        double waterVapourEfficiency = 0.45; //permeation efficiency of water vapour through the clothing layer
        double clothingVapourPermeationEfficiency = waterVapourEfficiency * convectiveHeatTransferCoeff
                / sumOfConvectiveAndRadiativeHeatTransferCoeff * (1 - ratioOfSurfacedClothedBodyOverNudeBody)
                        / (convectiveHeatTransferCoeff / sumOfConvectiveAndRadiativeHeatTransferCoeff - ratioOfSurfacedClothedBodyOverNudeBody * waterVapourEfficiency); //clothing vapor permeation efficiency
        double resistanceOfAirLayerToDryHeat = 1 / (increaseInBodySurfaceArea * sumOfConvectiveAndRadiativeHeatTransferCoeff);  //resistance of air layer to dry heat
        double evaporativeResistanceAirLayer = 1 / (lewisRatio * increaseInBodySurfaceArea * convectiveHeatTransferCoeff);
        double rEclS = thermalInsulationOfClothing / (lewisRatio * clothingVapourPermeationEfficiency);
        double hDS = 1 / (resistanceOfAirLayerToDryHeat + thermalInsulationOfClothing);
        double hES = 1 / (evaporativeResistanceAirLayer + rEclS);

        double delta = 0.0001;
        double dx = 100.0;
        double set = 0;
        double setOld = Math.round((skinTemp - totalHeatLossFromSkin / hDS) * 100) / 100.0;
        while (Math.abs(dx) > 0.01) {
            double err1 = totalHeatLossFromSkin
                            - hDS * (skinTemp - setOld)
                            - w
                            * hES
                            * (saturationVapourPressureOfWaterOfSkin - 0.5 * (Math.exp(18.6686 - 4030.183 / (setOld + 235))));
            double err2 = totalHeatLossFromSkin
                            - hDS * (skinTemp - (setOld + delta))
                            - w
                            * hES
                            * (saturationVapourPressureOfWaterOfSkin
                                    - 0.5 * (Math.exp(18.6686 - 4030.183 / (setOld + delta + 235))));
            set = setOld - delta * err1 / (err2 - err1);
            dx = set - setOld;
            setOld = set;
        }
        return set;
    }

    /**
     * Estimates the saturation vapor pressure in [torr]
     *
     * @param  airTemp  dry bulb air temperature, [C]
     * @return      saturation vapor pressure [torr]
     */
    private double getSaturationVaporPressure(double airTemp) {
        return Math.exp(18.6686 - 4030.183 / (airTemp + 235));
    }

    private double getRelativeSpeed(double airSpeed, double metabolicRate) {
        return metabolicRate > 1 ? Math.round((airSpeed + 0.3 * (metabolicRate - 1)) * 1000.0) / 1000.0 : airSpeed;
    }

    @Override
    public void destroy() {

    }
}
