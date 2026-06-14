// Fixture-based unit tests for src/classify.ts.
// Run: npx tsx scripts/test-classify.ts
//
// Cases are drawn from REAL catalog rows (especially the false-positive traps
// surfaced during data mining: "Shaggy Dog" sweaters, "Blanket Stripe"
// overshirts, "dress shirt" menswear, camping gear, etc.).

import { classifyItem, normalizeGender, type ClassifyInput, type Gender, type ProductType } from "../src/classify.js";

type Expect = {
  isClothing?: boolean;
  gender?: Gender;
  productType?: ProductType;
};
type Case = { desc: string; input: ClassifyInput; expect: Expect };

const cases: Case[] = [
  // --- Non-wearables that MUST be hidden -------------------------------------
  { desc: "wallet", input: { name: "Perfect Wallet - Signature Leather", category: "Wallets" }, expect: { isClothing: false } },
  { desc: "bifold wallet in generic accessories", input: { name: "Thread Benny Bifold Wallet", category: "accessories" }, expect: { isClothing: false } },
  { desc: "card case", input: { name: "Ganzo for Drake's Card Case", category: "SHOPIFY" }, expect: { isClothing: false } },
  { desc: "key fob", input: { name: "Captain's Knot Ribbon Key Fob", category: "Accessories (keychain)" }, expect: { isClothing: false } },
  { desc: "flask", input: { name: "Round Titanium Flask in 150 mL", category: "Flasks" }, expect: { isClothing: false } },
  { desc: "blanket", input: { name: "The Manchester Tartan Blanket", category: "Home (blankets)" }, expect: { isClothing: false } },
  { desc: "tent", input: { name: "Land Nest Dome Small", category: "Tents" }, expect: { isClothing: false } },
  { desc: "grill table", input: { name: "Deep Mesh Tray Half Unit", category: "Iron Grill Table" }, expect: { isClothing: false } },
  { desc: "cooler/container", input: { name: "Camping Bucket S", category: "Containers & Coolers" }, expect: { isClothing: false } },
  { desc: "cologne", input: { name: "Santal Eau de Parfum", category: "Colognes" }, expect: { isClothing: false } },
  { desc: "magazine", input: { name: "Secret Trips Magazine vol.1", category: "Publications-Rollover" }, expect: { isClothing: false } },
  { desc: "framed art print", input: { name: "October Home Framed Art Print", category: "Art Print Frame" }, expect: { isClothing: false } },
  { desc: "iron-on patch (standalone)", input: { name: "Tarpon Iron-On Patch", category: "Embroidered Patches" }, expect: { isClothing: false } },
  { desc: "pet collar", input: { name: "Clayton's Canopy Hammock Dog Collar", category: "Pet (collar)" }, expect: { isClothing: false } },
  { desc: "candle", input: { name: "Cedar & Sage Candle", category: "Home" }, expect: { isClothing: false } },
  { desc: "Coat Long Wallet (poisoned category=outerwear/coat)", input: { name: "OGL Kingsman Coat Long Wallet - Tumbled Black", category: "outerwear", subcategory: "coat", sourceUrl: "https://ironheart.co.uk/products/ogl-km-coatwal-tblk" }, expect: { isClothing: false } },
  { desc: "Incense Cones mis-tagged outerwear", input: { name: "Harvest | Japanese Yuzu Incense Cones", category: "outerwear" }, expect: { isClothing: false } },
  { desc: "Air Freshener mis-tagged MENS", input: { name: "Disband Air Freshener", category: "MENS" }, expect: { isClothing: false } },
  // head-noun override must NOT fire when the head is a real garment
  { desc: "Wallet Chain Jeans stay clothing", input: { name: "Wallet Chain Selvedge Jeans", category: "bottoms" }, expect: { isClothing: true, productType: "bottoms" } },
  { desc: "Roman Candle graphic tee stays clothing", input: { name: "Roman Candle Graphic Tee", category: "tops" }, expect: { isClothing: true, productType: "tops" } },
  { desc: "Blanket Jacket stays clothing", input: { name: "Cotton Jacquard Blanket Jacket", category: "outerwear" }, expect: { isClothing: true, productType: "jackets" } },
  { desc: "Tent T-Shirt stays clothing", input: { name: "Choose Your Tent T-Shirt", category: "tops" }, expect: { isClothing: true, productType: "tops" } },
  { desc: "Car Coat with Blanket Lining stays clothing", input: { name: "Brown Suede Car Coat with Blanket Lining", category: "outerwear", subcategory: "coat" }, expect: { isClothing: true, productType: "jackets" } },
  { desc: "Wool Blanket hides (literal head)", input: { name: "Lambswool Tartan Blanket", category: "Home" }, expect: { isClothing: false } },
  { desc: "Signet ring stays clothing (jewelry)", input: { name: "Sterling Signet Ring", category: "accessories" }, expect: { isClothing: true, productType: "accessories" } },
  { desc: "mug", input: { name: "Enamel Camp Mug", category: "Camping Cookware & Dinnerware" }, expect: { isClothing: false } },

  // --- False-positive traps: wearable signal must WIN a denylist mention -----
  { desc: "Shaggy Dog sweater (dog token)", input: { name: "Lilac Blue Shaggy Dog Sweater - Classic Fit", category: "tops" }, expect: { isClothing: true, productType: "tops" } },
  { desc: "Cat-O-Lantern sweater (lantern token)", input: { name: "Cat-O-Lantern Sweater", category: "Wmn's Sweaters" }, expect: { isClothing: true, productType: "tops", gender: "female" } },
  { desc: "Shaggy Dog baseball cap", input: { name: "Brown Dog Heavy Cotton Twill Baseball Cap", category: "Hats" }, expect: { isClothing: true, productType: "accessories" } },
  { desc: "Blanket Stripe overshirt", input: { name: "Altered Threads Overshirt - Multi Blanket Stripe", category: "BRX/Men/Clothing/Tops/Wovens" }, expect: { isClothing: true, productType: "tops", gender: "male" } },
  { desc: "Poster Art tee", input: { name: "Eddie Would Go Poster Art Tee Long Sleeve Black", category: "tops" }, expect: { isClothing: true, productType: "tops" } },
  { desc: "Bottle Green sweater (bottle color)", input: { name: "Bottle Green Lambswool Sweater", category: "Knitwear" }, expect: { isClothing: true, productType: "tops" } },
  { desc: "Sweater with elbow patches", input: { name: "Wool Sweater with Elbow Patches", category: "Men's Sweaters" }, expect: { isClothing: true, productType: "tops", gender: "male" } },
  { desc: "Coffee-colored shirt", input: { name: "Coffee Brown Oxford Shirt", category: "Casual Shirts" }, expect: { isClothing: true, productType: "tops" } },

  // --- Gender resolution -----------------------------------------------------
  { desc: "dress → female (not unisex)", input: { name: "Floral Midi Dress", category: "Wmn's Dresses (other)" }, expect: { isClothing: true, gender: "female" } },
  { desc: "sundress by name only", input: { name: "Tiered Cotton Sundress", category: "apparel" }, expect: { isClothing: true, gender: "female" } },
  { desc: "skirt → female", input: { name: "Pleated Wool Skirt", category: "Wmn's Skirt" }, expect: { isClothing: true, gender: "female", productType: "bottoms" } },
  { desc: "blouse → female", input: { name: "Silk Button Blouse", category: "apparel" }, expect: { isClothing: true, gender: "female", productType: "tops" } },
  { desc: "MEN's dress shirt stays male (dress-adj guard)", input: { name: "Men's White Dress Shirt", category: "Men's Shirts", sourceUrl: "https://x.com/mens/products/white-dress-shirt" }, expect: { isClothing: true, gender: "male", productType: "tops" } },
  { desc: "dress pants stay male-ish (no female)", input: { name: "Charcoal Dress Pants", category: "mens" }, expect: { isClothing: true, gender: "male", productType: "bottoms" } },
  { desc: "gender from URL /womens/", input: { name: "Linen Camp Shirt", sourceUrl: "https://brand.com/collections/womens/products/linen-camp-shirt", category: "apparel" }, expect: { gender: "female" } },
  { desc: "gender from URL /mens/", input: { name: "Linen Camp Shirt", sourceUrl: "https://brand.com/collections/mens/products/linen-camp-shirt", category: "apparel" }, expect: { gender: "male" } },
  { desc: "kids stays kids", input: { name: "Flannel Shirt", category: "Kid's Shirts (flannel)" }, expect: { isClothing: true, gender: "kids" } },
  { desc: "neutral tee → unisex", input: { name: "Heavyweight Pocket Tee", category: "t-shirts" }, expect: { isClothing: true, gender: "unisex", productType: "tops" } },
  { desc: "retailer default applies when no signal", input: { name: "Heavyweight Pocket Tee", category: "tops", retailerDefaultGender: "male" }, expect: { gender: "male" } },
  { desc: "garment female overrides retailer male default", input: { name: "Wrap Skirt", category: "bottoms", retailerDefaultGender: "male" }, expect: { gender: "female" } },

  // --- productType normalization --------------------------------------------
  { desc: "tie → accessories", input: { name: "Navy Patterned Silk Tie", category: "Patterned Ties" }, expect: { isClothing: true, productType: "accessories" } },
  { desc: "socks → accessories", input: { name: "Merino Crew Socks", category: "Accessories (socks)" }, expect: { isClothing: true, productType: "accessories" } },
  { desc: "tote → bags", input: { name: "Canvas Tote Bag", category: "accessories" }, expect: { isClothing: true, productType: "bags" } },
  { desc: "shoes → accessories", input: { name: "Suede Chukka Boots", category: "Footwear" }, expect: { isClothing: true, productType: "accessories" } },
  { desc: "coat → jackets", input: { name: "Wool Overcoat", category: "Coats & Jackets" }, expect: { isClothing: true, productType: "jackets" } },
  { desc: "jeans → bottoms", input: { name: "Slim Selvedge Jeans", category: "bottoms" }, expect: { isClothing: true, productType: "bottoms" } },
  { desc: "pocket square stays clothing", input: { name: "Paisley Silk Pocket Square", category: "accessories" }, expect: { isClothing: true, productType: "accessories" } },

  // --- Default-keep on unknown ----------------------------------------------
  { desc: "totally unknown → kept", input: { name: "Mystery Item XL", category: "mc5" }, expect: { isClothing: true } },
];

// normalizeGender unit checks
const normCases: Array<[string | null, Gender | null]> = [
  ["men", "male"], ["Mens", "male"], ["women", "female"], ["Women's", "female"],
  ["kids", "kids"], ["unisex", "unisex"], ["male", "male"], ["female", "female"],
  ["", null], ["weird", null], [null, null],
];

let pass = 0;
let fail = 0;
const failures: string[] = [];

for (const c of cases) {
  const r = classifyItem(c.input);
  const checks: string[] = [];
  if (c.expect.isClothing !== undefined && r.isClothing !== c.expect.isClothing) checks.push(`isClothing ${r.isClothing}≠${c.expect.isClothing}`);
  if (c.expect.gender !== undefined && r.gender !== c.expect.gender) checks.push(`gender ${r.gender}≠${c.expect.gender}`);
  if (c.expect.productType !== undefined && r.productType !== c.expect.productType) checks.push(`productType ${r.productType}≠${c.expect.productType}`);
  if (checks.length === 0) {
    pass++;
  } else {
    fail++;
    failures.push(`✗ ${c.desc}: ${checks.join(", ")}  | ${r.signal}`);
  }
}

for (const [raw, exp] of normCases) {
  const got = normalizeGender(raw);
  if (got === exp) pass++;
  else { fail++; failures.push(`✗ normalizeGender(${JSON.stringify(raw)})=${got}≠${exp}`); }
}

console.log(`\nclassify.ts tests: ${pass} passed, ${fail} failed (of ${pass + fail})\n`);
if (failures.length) {
  console.log(failures.join("\n"));
  process.exit(1);
}
console.log("✅ all green");
