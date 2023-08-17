import React from "react";
import clsx from "clsx";
import styles from "./styles.module.css";
import IconClipper from "@site/src/components/IconClipper";
import { iconArrayTechnology } from "../../constans/IconArray";
import Technology from "@site/static/img/technology.png";

type FeatureItem = {
  title: string;
  Svg: React.ComponentType<React.ComponentProps<"svg">>;
  description: JSX.Element;
};

const FeatureList: FeatureItem[] = [
  {
    title: "Cloud-first storage",
    Svg: 5,
    description: (
      <>
        Data is stored based on cloud infrastructure such as block storage and
        object storage provided by public cloud vendors, which brings great
        advantages in cost and reliability.
      </>
    ),
  },
  {
    title: "10x lower tail latencies",
    Svg: 4,
    description: (
      <>
        Up to 10x lower average latencies for the same workloads on identical
        hardware, even under sustained loads.
      </>
    ),
  },
  {
    title: "Designed in Rust",
    Svg: 9,
    description: (
      <>
        Thanks to the thread-per-core architecture design, it can give full play
        to the extreme performance of CPU, memory, IOPS, bandwidth and other
        resources.
      </>
    ),
  },
];

function Feature({ title, Svg, description }: FeatureItem) {
  return (
    <div className={clsx("col col--4")}>
      <div
        className="text--center"
        style={{ display: "flex", justifyContent: "center" }}
      >
        <IconClipper
          icons={iconArrayTechnology}
          imageUrl={Technology}
          originScale={3}
          scaledHeight={108}
          scaledWidth={108}
          targetIcon={Svg}
        />
      </div>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): JSX.Element {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
